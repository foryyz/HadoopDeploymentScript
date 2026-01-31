#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/hadoop-deploy-sc_2.log"

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
  - 负责 JDK/Hadoop 下载、安装、生成配置、分发到 worker（远程 root 执行）
  - 不执行 format/start/health_check（将由单独脚本负责）

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
  : "${CLUSTER_IPS:?}"

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

  # 新增：root 推送免密模式（推荐 sshpass）
  : "${SSH_PUSH_MODE:?}"          # copy-id | sshpass
  : "${SSH_DEFAULT_PASSWORD:?}"   # 仅 sshpass 时需要（用于克隆默认密码）
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
    getent hosts "${h}" >/dev/null 2>&1 || die "无法解析 ${h}，请确认三台已跑 sc_1.sh 并写好 /etc/hosts"
  done
}

# ---- SSH (root -> workers) ----
prepare_root_known_hosts() {
  mkdir -p /root/.ssh
  chmod 700 /root/.ssh
  touch /root/.ssh/known_hosts
  chmod 600 /root/.ssh/known_hosts

  local w
  for w in $(get_workers); do
    ssh-keygen -R "${w}" >/dev/null 2>&1 || true
    ssh-keyscan -H "${w}" >> /root/.ssh/known_hosts 2>/dev/null || true
  done
}

push_root_key_to_workers() {
  # root keypair
  if [[ ! -f /root/.ssh/id_rsa ]]; then
    ssh-keygen -t rsa -b 4096 -N "" -f /root/.ssh/id_rsa >/dev/null
  fi

  local w
  for w in $(get_workers); do
    if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
      log "ssh-copy-id root@${w}（需要输入 root 密码）"
      ssh-copy-id -o StrictHostKeyChecking=yes "root@${w}"
    elif [[ "${SSH_PUSH_MODE}" == "sshpass" ]]; then
      apt_install sshpass >/dev/null
      log "sshpass 推送 root 公钥到 root@${w}"
      sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh-copy-id -o StrictHostKeyChecking=yes "root@${w}"
    else
      die "未知 SSH_PUSH_MODE=${SSH_PUSH_MODE}（只支持 copy-id/sshpass）"
    fi
  done
}

# ---- SSH (hadoop -> workers) ----
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
  [[ -f "${uhome}/.ssh/id_rsa.pub" ]] || die "master 上 ${HADOOP_USER} 未生成 SSH key，请先跑 sc_1.sh"

  local w
  for w in $(get_workers); do
    if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
      log "ssh-copy-id ${HADOOP_USER}@${w}（需要输入 hadoop 密码）"
      sudo -u "${HADOOP_USER}" ssh-copy-id -o StrictHostKeyChecking=yes "${HADOOP_USER}@${w}"
    else
      : "${SSH_DEFAULT_PASSWORD:?SSH_PUSH_MODE=sshpass 需要 SSH_DEFAULT_PASSWORD}"
      log "sshpass 推送 ${HADOOP_USER} 公钥到 ${HADOOP_USER}@${w}"
      sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh-copy-id -o StrictHostKeyChecking=yes "${HADOOP_USER}@${w}"
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

generate_hadoop_configs() {
  local etc_dir="${HADOOP_SYMLINK}/etc/hadoop"
  [[ -d "${etc_dir}" ]] || die "找不到 ${etc_dir}"

  local secondary_port="9868"  # Hadoop3 SecondaryNameNode web default
  cat > "${etc_dir}/core-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_HOSTNAME}:${FS_DEFAULT_PORT}</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>${HADOOP_DATA_DIR}/tmp</value>
  </property>
</configuration>
EOF

  cat > "${etc_dir}/hdfs-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>${HDFS_REPLICATION}</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${HDFS_NAME_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HDFS_DATA_DIR}</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.secondary.http-address</name>
    <value>${SECONDARY_NAMENODE_HOSTNAME}:${secondary_port}</value>
  </property>
</configuration>
EOF

  cat > "${etc_dir}/yarn-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${MASTER_HOSTNAME}</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

  cat > "${etc_dir}/mapred-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>${JOBHISTORYSERVER_HOSTNAME}:${MAPREDUCE_JOBHISTORY_ADDRESS_PORT}</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>${JOBHISTORYSERVER_HOSTNAME}:${MAPREDUCE_JOBHISTORY_WEBAPP_PORT}</value>
  </property>
</configuration>
EOF

  cat > "${etc_dir}/workers" <<EOF
${WORKER1_HOSTNAME}
${WORKER2_HOSTNAME}
EOF

  # JAVA_HOME into hadoop-env.sh (idempotent marker)
  local env_file="${etc_dir}/hadoop-env.sh"
  sed -i '/# BEGIN HADOOP_CLUSTER_JAVA_HOME/,/# END HADOOP_CLUSTER_JAVA_HOME/d' "${env_file}" || true
  cat >> "${env_file}" <<EOF

# BEGIN HADOOP_CLUSTER_JAVA_HOME
export JAVA_HOME="${JAVA_DIR}"
# END HADOOP_CLUSTER_JAVA_HOME
EOF

  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${etc_dir}" || true
  log "Hadoop 配置生成完成。"
}

# ---- Distribute to workers (root) ----
remote_sudo() {
  local host="$1"; shift
  local cmd="$*"

  if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
    # 交互式：会提示输入 sudo 密码
    sudo -u "${HADOOP_USER}" ssh -tt "${HADOOP_USER}@${host}" "sudo bash -lc $(printf '%q' "${cmd}")"
  else
    # 全自动：用 sshpass 提供 sudo 密码
    : "${SSH_DEFAULT_PASSWORD:?SSH_PUSH_MODE=sshpass 需要 SSH_DEFAULT_PASSWORD}"
    sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh -tt "${HADOOP_USER}@${host}" "echo '${SSH_DEFAULT_PASSWORD}' | sudo -S bash -lc $(printf '%q' "${cmd}")"
  fi
}

distribute_to_workers_hadoop() {
  local version_dir
  version_dir="$(readlink -f "${HADOOP_SYMLINK}")"

  local w
  for w in $(get_workers); do
    log "=== 分发到 ${w}（hadoop@worker）==="

    # 1) 先在用户目录临时接收
    sudo -u "${HADOOP_USER}" ssh "${HADOOP_USER}@${w}" "mkdir -p /home/${HADOOP_USER}/.stage_hadoop" >/dev/null

    log "rsync JDK 到 ${w} 的临时目录..."
    sudo -u "${HADOOP_USER}" rsync -az --delete "${JAVA_DIR}/" "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/jdk/"

    log "rsync Hadoop 到 ${w} 的临时目录..."
    sudo -u "${HADOOP_USER}" rsync -az --delete "${version_dir}/" "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/hadoop/"

    log "rsync profile.d 脚本到 ${w} 临时目录..."
    sudo -u "${HADOOP_USER}" rsync -az /etc/profile.d/java.sh "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/java.sh"
    sudo -u "${HADOOP_USER}" rsync -az /etc/profile.d/hadoop.sh "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop/hadoop.sh"

    # 2) 用 sudo 把临时目录内容搬到系统目录
    log "=== 分发jdk和hadoop到系统目录==="
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
    "

    log "${w} 分发完成。"
  done
}

distribute_to_workers_root() {
  local version_dir
  version_dir="$(readlink -f "${HADOOP_SYMLINK}")"

  local w
  for w in $(get_workers); do
    log "=== 分发到 ${w}（root）==="

    # ensure base dirs
    ssh -o BatchMode=yes "root@${w}" "mkdir -p '${INSTALL_BASE}' '${HADOOP_DATA_DIR}' '${HDFS_NAME_DIR}' '${HDFS_DATA_DIR}'"

    # rsync Java + Hadoop version dir
    rsync -az --delete "${JAVA_DIR}/" "root@${w}:${JAVA_DIR}/"
    rsync -az --delete "${version_dir}/" "root@${w}:${version_dir}/"

    # symlink
    ssh -o BatchMode=yes "root@${w}" "rm -f '${HADOOP_SYMLINK}' && ln -s '${version_dir}' '${HADOOP_SYMLINK}'"

    # profile.d
    rsync -az /etc/profile.d/java.sh "root@${w}:/etc/profile.d/java.sh"
    rsync -az /etc/profile.d/hadoop.sh "root@${w}:/etc/profile.d/hadoop.sh"
    ssh -o BatchMode=yes "root@${w}" "chmod 644 /etc/profile.d/java.sh /etc/profile.d/hadoop.sh"

    # ensure hadoop user owns needed dirs
    ssh -o BatchMode=yes "root@${w}" "id -u '${HADOOP_USER}' >/dev/null 2>&1 || useradd -m -s /bin/bash '${HADOOP_USER}'"
    ssh -o BatchMode=yes "root@${w}" "chown -R '${HADOOP_USER}:${HADOOP_USER}' '${HADOOP_DATA_DIR}' '${version_dir}'"

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

  # prepare_root_known_hosts
  # push_root_key_to_workers
  prepare_hadoop_known_hosts
  push_hadoop_key_to_workers

  local files
  files="$(download_artifacts)"
  local jdk_tar="${files%%|*}"
  local hdp_tar="${files##*|}"

  install_jdk_local "${jdk_tar}"
  install_hadoop_local "${hdp_tar}"

  # create data dirs local
  mkdir -p "${HADOOP_DATA_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DATA_DIR}" || true

  generate_hadoop_configs
  # distribute_to_workers_root
  distribute_to_workers_hadoop

  log "DONE: sc_2 完成安装+配置+分发"
  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"
