#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/hadoop-deploy-sc_master.log"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE" >&2; }
die() { log "ERROR: $*"; exit 1; }

trap 'ec=$?; log "ERROR(exit=$ec) line ${BASH_LINENO[0]}: ${BASH_COMMAND}"; exit $ec' ERR

CONF_PATH=""
FORCE_REINSTALL="false"

usage() {
  cat >&2 <<EOF
用法:
  sudo ./${SCRIPT_NAME} [--conf /path/cluster.conf] [--force]

说明:
  - 仅在 master 上执行
  - 负责：下载/安装 JDK+Hadoop、生成配置、通过 hadoop@worker 分发并落地
  - 不执行 format/start/health_check（由 run_hadoop.sh 负责）

可选:
  --conf <path>   指定配置文件（默认: 脚本同目录 cluster.conf）
  --force         强制覆盖安装目录（谨慎）
EOF
}

require_root() {
  [[ "${EUID}" -eq 0 ]] || die "请用 root 或 sudo 运行：sudo ./${SCRIPT_NAME} ..."
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --conf) CONF_PATH="$2"; shift 2;;
      --force) FORCE_REINSTALL="true"; shift 1;;
      -h|--help) usage; exit 0;;
      *) die "未知参数: $1";;
    esac
  done
}

apt_install() {
  local pkgs=("$@")
  export DEBIAN_FRONTEND=noninteractive
  log "apt update..."
  apt-get update -y >/dev/null
  log "apt install: ${pkgs[*]} ..."
  apt-get install -y "${pkgs[@]}" >/dev/null
}

load_config() {
  if [[ -z "${CONF_PATH}" ]]; then
    [[ -f "${WORKDIR}/cluster.conf" ]] || die "找不到 cluster.conf，请用 --conf 指定"
    CONF_PATH="${WORKDIR}/cluster.conf"
  fi
  # shellcheck disable=SC1090
  source "${CONF_PATH}"

  : "${HADOOP_USER:?}"
  : "${MASTER_HOSTNAME:?}"
  : "${WORKER1_HOSTNAME:?}"
  : "${WORKER2_HOSTNAME:?}"
  : "${CLUSTER_HOSTNAMES:?}"

  : "${JDK_DOWNLOAD_LINK:?}"
  : "${HADOOP_DOWNLOAD_LINK:?}"

  : "${INSTALL_BASE:?}"
  : "${JAVA_DIR:?}"
  : "${HADOOP_DIR:?}"
  : "${HADOOP_SYMLINK:?}"

  : "${HADOOP_DATA_DIR:?}"
  : "${HDFS_NAME_DIR:?}"
  : "${HDFS_DATA_DIR:?}"

  : "${FS_DEFAULT_PORT:?}"
  : "${HDFS_REPLICATION:?}"

  : "${SECONDARY_NAMENODE_HOSTNAME:?}"
  : "${JOBHISTORYSERVER_HOSTNAME:?}"
  : "${MAPREDUCE_JOBHISTORY_ADDRESS_PORT:?}"
  : "${MAPREDUCE_JOBHISTORY_WEBAPP_PORT:?}"

  : "${SSH_PUSH_MODE:?}"          # copy-id | sshpass
  : "${SSH_DEFAULT_PASSWORD:?}"   # 仅 sshpass 时需要
}

require_master() {
  local hn
  hn="$(hostnamectl --static 2>/dev/null || hostname)"
  [[ "${hn}" == "${MASTER_HOSTNAME}" ]] || die "只能在 master 执行：当前=${hn} 期望=${MASTER_HOSTNAME}"
}

get_workers() { echo "${WORKER1_HOSTNAME} ${WORKER2_HOSTNAME}"; }

ensure_hadoop_user_exists_local() {
  if id -u "${HADOOP_USER}" >/dev/null 2>&1; then return 0; fi
  useradd -m -s /bin/bash "${HADOOP_USER}"
}

assert_hosts_ready() {
  local h
  for h in "${CLUSTER_HOSTNAMES[@]}"; do
    getent hosts "${h}" >/dev/null 2>&1 || die "无法解析 ${h}，请确认三台已跑 sc_all.sh 并写好 /etc/hosts"
  done
}

# ---- SSH: hadoop -> workers ----
prepare_hadoop_known_hosts() {
  local uhome
  uhome="$(eval echo "~${HADOOP_USER}")"
  mkdir -p "${uhome}/.ssh"
  chmod 700 "${uhome}/.ssh"
  touch "${uhome}/.ssh/known_hosts"
  chmod 600 "${uhome}/.ssh/known_hosts"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${uhome}/.ssh"

  local w
  for w in $(get_workers); do
    sudo -u "${HADOOP_USER}" ssh-keygen -R "${w}" >/dev/null 2>&1 || true
    sudo -u "${HADOOP_USER}" ssh-keyscan -H "${w}" >> "${uhome}/.ssh/known_hosts" 2>/dev/null || true
  done
}

push_hadoop_key_to_workers() {
  local uhome
  uhome="$(eval echo "~${HADOOP_USER}")"
  [[ -f "${uhome}/.ssh/id_rsa.pub" ]] || die "master 上 ${HADOOP_USER} 未生成 SSH key，请先跑 sc_all.sh"

  local w
  for w in $(get_workers); do
    if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
      log "ssh-copy-id ${HADOOP_USER}@${w}（需要输入 ${HADOOP_USER} 密码）"
      sudo -u "${HADOOP_USER}" ssh-copy-id -o StrictHostKeyChecking=yes "${HADOOP_USER}@${w}"
    elif [[ "${SSH_PUSH_MODE}" == "sshpass" ]]; then
      apt_install sshpass >/dev/null
      log "sshpass 推送 ${HADOOP_USER} 公钥到 ${HADOOP_USER}@${w}"
      sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh-copy-id -o StrictHostKeyChecking=yes "${HADOOP_USER}@${w}"
    else
      die "未知 SSH_PUSH_MODE=${SSH_PUSH_MODE}（只支持 copy-id/sshpass）"
    fi
  done
}

# ---- Download ----
fname() { echo "${1##*/}"; }

download_artifacts() {
  mkdir -p "${INSTALL_BASE}/src"
  local jdk_tar="${INSTALL_BASE}/src/$(fname "${JDK_DOWNLOAD_LINK}")"
  local hdp_tar="${INSTALL_BASE}/src/$(fname "${HADOOP_DOWNLOAD_LINK}")"

  if [[ ! -f "${jdk_tar}" ]]; then
    log "下载 JDK..."
    curl -L --fail -o "${jdk_tar}" "${JDK_DOWNLOAD_LINK}"
  else
    log "JDK 包已存在：${jdk_tar}"
  fi

  if [[ ! -f "${hdp_tar}" ]]; then
    log "下载 Hadoop..."
    curl -L --fail -o "${hdp_tar}" "${HADOOP_DOWNLOAD_LINK}"
  else
    log "Hadoop 包已存在：${hdp_tar}"
  fi

  tar -tzf "${jdk_tar}" >/dev/null
  tar -tzf "${hdp_tar}" >/dev/null

  echo "${jdk_tar}|${hdp_tar}"
}

install_jdk_local() {
  local jdk_tar="$1"

  if [[ -d "${JAVA_DIR}" && "${FORCE_REINSTALL}" != "true" ]]; then
    log "JAVA_DIR 已存在，跳过：${JAVA_DIR}"
  else
    log "安装 JDK 到 ${JAVA_DIR}"
    rm -rf "${JAVA_DIR}"
    mkdir -p "${INSTALL_BASE}/.tmp_jdk"
    rm -rf "${INSTALL_BASE}/.tmp_jdk/*" || true
    tar -xzf "${jdk_tar}" -C "${INSTALL_BASE}/.tmp_jdk"
    local top
    top="$(find "${INSTALL_BASE}/.tmp_jdk" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    [[ -n "${top}" ]] || die "JDK 包结构异常"
    mv "${top}" "${JAVA_DIR}"
    rm -rf "${INSTALL_BASE}/.tmp_jdk"
  fi

  cat > /etc/profile.d/java.sh <<EOF
export JAVA_HOME="${JAVA_DIR}"
export PATH="\$JAVA_HOME/bin:\$PATH"
EOF
  chmod 644 /etc/profile.d/java.sh

  "${JAVA_DIR}/bin/java" -version >/dev/null 2>&1 || die "JDK 验证失败"
  log "JDK 安装完成。"
}

install_hadoop_local() {
  local hdp_tar="$1"

  mkdir -p "${INSTALL_BASE}/.tmp_hadoop"
  rm -rf "${INSTALL_BASE}/.tmp_hadoop/*" || true
  tar -xzf "${hdp_tar}" -C "${INSTALL_BASE}/.tmp_hadoop"
  local top
  top="$(find "${INSTALL_BASE}/.tmp_hadoop" -mindepth 1 -maxdepth 1 -type d | head -n1)"
  [[ -n "${top}" ]] || die "Hadoop 包结构异常"
  local version_dir="${HADOOP_DIR}-$(basename "${top}" | sed 's/^hadoop-//')"

  if [[ -d "${version_dir}" && "${FORCE_REINSTALL}" != "true" ]]; then
    log "Hadoop 已存在，跳过：${version_dir}"
    rm -rf "${INSTALL_BASE}/.tmp_hadoop"
  else
    log "安装 Hadoop 到 ${version_dir}"
    rm -rf "${version_dir}"
    mv "${top}" "${version_dir}"
    rm -rf "${INSTALL_BASE}/.tmp_hadoop"
  fi

  rm -f "${HADOOP_SYMLINK}"
  ln -s "${version_dir}" "${HADOOP_SYMLINK}"

  cat > /etc/profile.d/hadoop.sh <<EOF
export HADOOP_HOME="${HADOOP_SYMLINK}"
export PATH="\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH"
EOF
  chmod 644 /etc/profile.d/hadoop.sh

  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${version_dir}" || true
  log "Hadoop 安装完成：${HADOOP_SYMLINK} -> ${version_dir}"
}

refresh_env_local() {
  log "刷新本机环境变量（source /etc/profile.d/java.sh 与 /etc/profile.d/hadoop.sh）..."

  # shellcheck disable=SC1091
  source /etc/profile.d/java.sh || true
  # shellcheck disable=SC1091
  source /etc/profile.d/hadoop.sh || true

  # 验证（不强制失败，避免环境差异导致脚本中断）
  command -v java >/dev/null 2>&1 && log "java 已可用：$(java -version 2>&1 | head -n1)" || log "警告：当前 shell 未检测到 java（新登录后一定生效）"
  command -v hadoop >/dev/null 2>&1 && log "hadoop 已可用：$(hadoop version 2>/dev/null | head -n1)" || log "警告：当前 shell 未检测到 hadoop（新登录后一定生效）"
}

backup_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  # 同一路径备份一份，便于回滚
  cp -a "$f" "${f}.bak.$(date +%Y%m%d%H%M%S)"
}

ensure_xml_skeleton() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    cat >"$f" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
</configuration>
EOF
    return 0
  fi

  # 文件存在但缺 <configuration>（极少见），则保守：不破坏原内容，包一层 configuration
  if ! grep -q "<configuration>" "$f"; then
    backup_file "$f"
    cat >"${f}.tmp.$$" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
$(cat "$f")
</configuration>
EOF
    mv "${f}.tmp.$$" "$f"
  fi

  # 缺 </configuration> 也补上
  if ! grep -q "</configuration>" "$f"; then
    backup_file "$f"
    printf '\n</configuration>\n' >>"$f"
  fi
}

xml_upsert_property() {
  local f="$1" name="$2" value="$3"

  ensure_xml_skeleton "$f"
  backup_file "$f"

  # 用 perl 读整个文件做正则替换/插入（-0777 slurp）
  perl -0777 -i -pe '
    my ($name,$value)=@ARGV; shift @ARGV; shift @ARGV;

    my $block = "  <property>\n"
              . "    <name>$name</name>\n"
              . "    <value>$value</value>\n"
              . "  </property>\n";

    if (m{<property>\s*<name>\s*\Q$name\E\s*</name>.*?</property>}s) {
      s{<property>\s*<name>\s*\Q$name\E\s*</name>.*?</property>}{$block}s;
    } else {
      s{</configuration>}{$block</configuration>}s;
    }
  ' "$name" "$value" "$f"
}

# 针对纯文本列表文件（workers）用 marker 块替换，避免覆盖其它内容/注释
upsert_marker_block() {
  local f="$1" begin="$2" end="$3" content="$4"
  [[ -f "$f" ]] || touch "$f"
  backup_file "$f"
  # 删除旧 marker 块
  sed -i "/${begin}/,/${end}/d" "$f" || true
  # 追加新 marker 块
  cat >>"$f" <<EOF

${begin}
${content}
${end}
EOF
}

generate_hadoop_configs() {
  local etc_dir="${HADOOP_SYMLINK}/etc/hadoop"
  [[ -d "${etc_dir}" ]] || die "找不到 ${etc_dir}"

  local secondary_port="9868"  # Hadoop3 SecondaryNameNode web default

  # --- core-site.xml：只改动指定 property，不整文件覆盖 ---
  local core="${etc_dir}/core-site.xml"
  xml_upsert_property "${core}" "fs.defaultFS" "hdfs://${MASTER_HOSTNAME}:${FS_DEFAULT_PORT}"
  xml_upsert_property "${core}" "hadoop.tmp.dir" "${HADOOP_DATA_DIR}/tmp"

  # --- hdfs-site.xml ---
  local hdfs="${etc_dir}/hdfs-site.xml"
  xml_upsert_property "${hdfs}" "dfs.replication" "${HDFS_REPLICATION}"
  xml_upsert_property "${hdfs}" "dfs.namenode.name.dir" "file://${HDFS_NAME_DIR}"
  xml_upsert_property "${hdfs}" "dfs.datanode.data.dir" "file://${HDFS_DATA_DIR}"
  xml_upsert_property "${hdfs}" "dfs.permissions.enabled" "false"
  xml_upsert_property "${hdfs}" "dfs.namenode.datanode.registration.ip-hostname-check" "false"
  xml_upsert_property "${hdfs}" "dfs.namenode.secondary.http-address" "${SECONDARY_NAMENODE_HOSTNAME}:${secondary_port}"

  # --- yarn-site.xml ---
  local yarn="${etc_dir}/yarn-site.xml"
  xml_upsert_property "${yarn}" "yarn.resourcemanager.hostname" "${MASTER_HOSTNAME}"
  xml_upsert_property "${yarn}" "yarn.nodemanager.aux-services" "mapreduce_shuffle"

  # --- mapred-site.xml：优先从 template 生成一次，然后 upsert ---
  local mapred="${etc_dir}/mapred-site.xml"
  if [[ ! -f "${mapred}" && -f "${etc_dir}/mapred-site.xml.template" ]]; then
    cp -a "${etc_dir}/mapred-site.xml.template" "${mapred}"
  fi
  xml_upsert_property "${mapred}" "mapreduce.framework.name" "yarn"
  xml_upsert_property "${mapred}" "mapreduce.jobhistory.address" "${JOBHISTORYSERVER_HOSTNAME}:${MAPREDUCE_JOBHISTORY_ADDRESS_PORT}"
  xml_upsert_property "${mapred}" "mapreduce.jobhistory.webapp.address" "${JOBHISTORYSERVER_HOSTNAME}:${MAPREDUCE_JOBHISTORY_WEBAPP_PORT}"

  # --- workers：用 marker 块写入，避免覆盖其它内容/注释 ---
  local workers_file="${etc_dir}/workers"
  upsert_marker_block "${workers_file}" \
    "# BEGIN HADOOP_CLUSTER_WORKERS" \
    "# END HADOOP_CLUSTER_WORKERS" \
"${WORKER1_HOSTNAME}
${WORKER2_HOSTNAME}"

  # --- JAVA_HOME into hadoop-env.sh（你原来这段已经正确：marker 幂等） ---
  local env_file="${etc_dir}/hadoop-env.sh"
  sed -i '/# BEGIN HADOOP_CLUSTER_JAVA_HOME/,/# END HADOOP_CLUSTER_JAVA_HOME/d' "${env_file}" || true
  cat >> "${env_file}" <<EOF

# BEGIN HADOOP_CLUSTER_JAVA_HOME
export JAVA_HOME="${JAVA_DIR}"
# END HADOOP_CLUSTER_JAVA_HOME
EOF

  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${etc_dir}" || true
  log "Hadoop 配置已按 property 级别更新完成（保留原文件其它配置）。"
}

# ---- Distribute via hadoop@worker + sudo ----
remote_sudo() {
  local host="$1"; shift
  local cmd="$*"

  if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
    # 交互式：会提示输入 sudo 密码
    sudo -u "${HADOOP_USER}" ssh -tt "${HADOOP_USER}@${host}" "sudo bash -lc $(printf '%q' "${cmd}")"
  else
    # 全自动：用 sshpass 提供 sudo 密码
    apt_install sshpass >/dev/null
    sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh -tt "${HADOOP_USER}@${host}" \
      "echo '${SSH_DEFAULT_PASSWORD}' | sudo -S bash -lc $(printf '%q' "${cmd}")"
  fi
}

distribute_to_workers() {
  local version_dir
  version_dir="$(readlink -f "${HADOOP_SYMLINK}")"

  local w
  for w in $(get_workers); do
    log "=== 分发到 ${w}（hadoop@worker）==="

    sudo -u "${HADOOP_USER}" ssh "${HADOOP_USER}@${w}" "mkdir -p /home/${HADOOP_USER}/.stage_hadoop" >/dev/null

    log "rsync JDK -> ${w} 临时目录"
    sudo -u "${HADOOP_USER}" rsync -az --delete "${JAVA_DIR}/" "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/jdk/"

    log "rsync Hadoop -> ${w} 临时目录"
    sudo -u "${HADOOP_USER}" rsync -az --delete "${version_dir}/" "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/hadoop/"

    log "rsync profile.d -> ${w} 临时目录"
    sudo -u "${HADOOP_USER}" rsync -az /etc/profile.d/java.sh "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/java.sh"
    sudo -u "${HADOOP_USER}" rsync -az /etc/profile.d/hadoop.sh "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/hadoop.sh"

    log "落地到系统目录（sudo）"
    remote_sudo "${w}" "
      mkdir -p '${INSTALL_BASE}' '${HADOOP_DATA_DIR}' '${HDFS_NAME_DIR}' '${HDFS_DATA_DIR}';
      rm -rf '${JAVA_DIR}' '${version_dir}';
      mkdir -p '${JAVA_DIR}' '${version_dir}';
      rsync -a --delete /home/${HADOOP_USER}/.stage_hadoop/jdk/ '${JAVA_DIR}/';
      rsync -a --delete /home/${HADOOP_USER}/.stage_hadoop/hadoop/ '${version_dir}/';
      rm -f '${HADOOP_SYMLINK}';
      ln -s '${version_dir}' '${HADOOP_SYMLINK}';
      mv /home/${HADOOP_USER}/.stage_hadoop/java.sh /etc/profile.d/java.sh;
      mv /home/${HADOOP_USER}/.stage_hadoop/hadoop.sh /etc/profile.d/hadoop.sh;
      chmod 644 /etc/profile.d/java.sh /etc/profile.d/hadoop.sh;
      id -u '${HADOOP_USER}' >/dev/null 2>&1 || useradd -m -s /bin/bash '${HADOOP_USER}';
      chown -R '${HADOOP_USER}:${HADOOP_USER}' '${HADOOP_DATA_DIR}' '${version_dir}';
      # 远端立即验证 profile.d 可被 login shell 加载
      bash -lc 'source /etc/profile.d/java.sh; source /etc/profile.d/hadoop.sh; command -v java >/dev/null && command -v hadoop >/dev/null' || true
    "

    log "${w} 分发完成。"
  done
}

main() {
  require_root
  parse_args "$@"
  load_config
  require_master

  log "========== ${SCRIPT_NAME} START =========="
  log "conf: ${CONF_PATH}"

  apt_install openssh-client rsync curl wget tar ca-certificates openssh-server
  systemctl enable --now ssh >/dev/null 2>&1 || true

  assert_hosts_ready
  ensure_hadoop_user_exists_local

  prepare_hadoop_known_hosts
  push_hadoop_key_to_workers

  local files
  files="$(download_artifacts)"
  local jdk_tar="${files%%|*}"
  local hdp_tar="${files##*|}"

  install_jdk_local "${jdk_tar}"
  install_hadoop_local "${hdp_tar}"
  refresh_env_local


  mkdir -p "${HADOOP_DATA_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DATA_DIR}" || true

  generate_hadoop_configs
  distribute_to_workers

  log "DONE: sc_master 完成安装+配置+分发"
  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"
