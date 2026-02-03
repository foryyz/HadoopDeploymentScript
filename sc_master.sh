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
RUN_MODULE=""

usage() {
  cat >&2 <<EOF
用法:
  sudo ./${SCRIPT_NAME} [--conf /path/cluster.conf] [--force] [--module <ssh|install|config|all>]

说明:
  - 仅在 master 上执行
  - 通过菜单选择模块，或用 --module 直接运行

模块:
  ssh      配置 hadoop 用户免密登录到 worker
  install  下载/安装 JDK+Hadoop，写 profile.d，并分发到 worker
  config   从 cluster.conf 写 Hadoop 配置文件并分发
  all      依次执行 ssh -> install -> config

可选:
  --conf <path>   指定配置文件（默认: 脚本同目录 cluster.conf）
  --force         强制覆盖安装目录（谨慎）
  -h, --help      帮助
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
      --module) RUN_MODULE="$2"; shift 2;;
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

  : "${SSH_PUSH_MODE:?}"          # copy-id | sshpass
  : "${SSH_DEFAULT_PASSWORD:?}"   # sshpass 时需要

  : "${CORE_SITE_XML_CONTENT:?}"
  : "${HDFS_SITE_XML_CONTENT:?}"
  : "${YARN_SITE_XML_CONTENT:?}"
  : "${MAPRED_SITE_XML_CONTENT:?}"
  : "${WORKERS_FILE_CONTENT:?}"
  : "${HADOOP_ENV_SH_CONTENT:?}"
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
    getent hosts "${h}" >/dev/null 2>&1 || die "无法解析 ${h}，请确认三台已运行 sc_all.sh 并写好 /etc/hosts"
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
      log "ssh-copy-id ${HADOOP_USER}@${w}（需输入 ${HADOOP_USER} 密码）"
      sudo -u "${HADOOP_USER}" ssh-copy-id -o StrictHostKeyChecking=yes "${HADOOP_USER}@${w}"
    elif [[ "${SSH_PUSH_MODE}" == "sshpass" ]]; then
      apt_install sshpass >/dev/null
      log "sshpass 推送 ${HADOOP_USER} 公钥到 ${HADOOP_USER}@${w}"
      sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh-copy-id -o StrictHostKeyChecking=yes "${HADOOP_USER}@${w}"
    else
      die "未知 SSH_PUSH_MODE=${SSH_PUSH_MODE}（仅支持 copy-id/sshpass）"
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
  log "JDK 安装完成"
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
  log "刷新本机环境变量（source /etc/profile.d/java.sh + /etc/profile.d/hadoop.sh）..."

  # shellcheck disable=SC1091
  source /etc/profile.d/java.sh || true
  # shellcheck disable=SC1091
  source /etc/profile.d/hadoop.sh || true

  command -v java >/dev/null 2>&1 && log "java 可用：$(java -version 2>&1 | head -n1)" || log "警告：当前 shell 未检测到 java（重新登录后生效）"
  command -v hadoop >/dev/null 2>&1 && log "hadoop 可用：$(hadoop version 2>/dev/null | head -n1)" || log "警告：当前 shell 未检测到 hadoop（重新登录后生效）"
}

backup_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  cp -a "$f" "${f}.bak.$(date +%Y%m%d%H%M%S)"
}

write_file_content() {
  local f="$1"
  local content="$2"
  mkdir -p "$(dirname "$f")"
  backup_file "$f"
  printf "%s" "${content}" > "$f"
}

# ---- Distribute via hadoop@worker + sudo ----
remote_sudo() {
  local host="$1"; shift
  local cmd="$*"

  if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
    sudo -u "${HADOOP_USER}" ssh -tt "${HADOOP_USER}@${host}" "sudo bash -lc $(printf '%q' "${cmd}")"
  else
    apt_install sshpass >/dev/null
    sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh -tt "${HADOOP_USER}@${host}" \
      "echo '${SSH_DEFAULT_PASSWORD}' | sudo -S bash -lc $(printf '%q' "${cmd}")"
  fi
}

distribute_binaries_to_workers() {
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
      bash -lc 'source /etc/profile.d/java.sh; source /etc/profile.d/hadoop.sh; command -v java >/dev/null && command -v hadoop >/dev/null' || true
    "

    log "${w} 分发完成"
  done
}

distribute_configs_to_workers() {
  local etc_dir="${HADOOP_SYMLINK}/etc/hadoop"
  [[ -d "${etc_dir}" ]] || die "找不到 ${etc_dir}，请先执行 install 模块"

  local w
  for w in $(get_workers); do
    log "=== 分发配置到 ${w} ==="

    sudo -u "${HADOOP_USER}" ssh "${HADOOP_USER}@${w}" "mkdir -p /home/${HADOOP_USER}/.stage_hadoop_cfg" >/dev/null

    sudo -u "${HADOOP_USER}" rsync -az \
      "${etc_dir}/core-site.xml" \
      "${etc_dir}/hdfs-site.xml" \
      "${etc_dir}/yarn-site.xml" \
      "${etc_dir}/mapred-site.xml" \
      "${etc_dir}/workers" \
      "${etc_dir}/hadoop-env.sh" \
      "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_hadoop_cfg/"

    remote_sudo "${w}" "
      test -d '${HADOOP_SYMLINK}/etc/hadoop' || exit 1;
      install -m 644 /home/${HADOOP_USER}/.stage_hadoop_cfg/core-site.xml '${HADOOP_SYMLINK}/etc/hadoop/core-site.xml';
      install -m 644 /home/${HADOOP_USER}/.stage_hadoop_cfg/hdfs-site.xml '${HADOOP_SYMLINK}/etc/hadoop/hdfs-site.xml';
      install -m 644 /home/${HADOOP_USER}/.stage_hadoop_cfg/yarn-site.xml '${HADOOP_SYMLINK}/etc/hadoop/yarn-site.xml';
      install -m 644 /home/${HADOOP_USER}/.stage_hadoop_cfg/mapred-site.xml '${HADOOP_SYMLINK}/etc/hadoop/mapred-site.xml';
      install -m 644 /home/${HADOOP_USER}/.stage_hadoop_cfg/workers '${HADOOP_SYMLINK}/etc/hadoop/workers';
      install -m 644 /home/${HADOOP_USER}/.stage_hadoop_cfg/hadoop-env.sh '${HADOOP_SYMLINK}/etc/hadoop/hadoop-env.sh';
      chown -R '${HADOOP_USER}:${HADOOP_USER}' '${HADOOP_SYMLINK}/etc/hadoop';
      rm -rf /home/${HADOOP_USER}/.stage_hadoop_cfg
    "

    log "${w} 配置分发完成"
  done
}

# ---- Modules ----
module_ssh() {
  log "=== Module: SSH免密配置 ==="
  apt_install openssh-client openssh-server
  systemctl enable --now ssh >/dev/null 2>&1 || true

  assert_hosts_ready
  ensure_hadoop_user_exists_local
  prepare_hadoop_known_hosts
  push_hadoop_key_to_workers
}

module_install() {
  log "=== Module: 下载/安装/分发 JDK+Hadoop ==="
  apt_install openssh-client rsync curl wget tar ca-certificates openssh-server
  systemctl enable --now ssh >/dev/null 2>&1 || true

  assert_hosts_ready
  ensure_hadoop_user_exists_local

  local files
  files="$(download_artifacts)"
  local jdk_tar="${files%%|*}"
  local hdp_tar="${files##*|}"

  install_jdk_local "${jdk_tar}"
  install_hadoop_local "${hdp_tar}"
  refresh_env_local

  mkdir -p "${HADOOP_DATA_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DATA_DIR}" || true

  distribute_binaries_to_workers
}

module_config() {
  log "=== Module: 写入 Hadoop 配置并分发 ==="
  local etc_dir="${HADOOP_SYMLINK}/etc/hadoop"
  [[ -d "${etc_dir}" ]] || die "找不到 ${etc_dir}，请先执行 install 模块"

  write_file_content "${etc_dir}/core-site.xml" "${CORE_SITE_XML_CONTENT}"
  write_file_content "${etc_dir}/hdfs-site.xml" "${HDFS_SITE_XML_CONTENT}"
  write_file_content "${etc_dir}/yarn-site.xml" "${YARN_SITE_XML_CONTENT}"
  write_file_content "${etc_dir}/mapred-site.xml" "${MAPRED_SITE_XML_CONTENT}"
  write_file_content "${etc_dir}/workers" "${WORKERS_FILE_CONTENT}"
  write_file_content "${etc_dir}/hadoop-env.sh" "${HADOOP_ENV_SH_CONTENT}"

  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${etc_dir}" || true
  distribute_configs_to_workers
}

run_module() {
  case "$1" in
    ssh) module_ssh;;
    install) module_install;;
    config) module_config;;
    all)
      module_ssh
      module_install
      module_config
      ;;
    *) die "未知模块: $1";;
  esac
}

menu_loop() {
  while true; do
    cat <<EOF

==============================
Hadoop Cluster Master Menu
==============================
1) SSH免密配置
2) 下载/安装/分发 JDK+Hadoop（含 /etc/profile.d）
3) 写入 Hadoop 配置文件（从 cluster.conf）并分发
4) 全部执行（1 -> 2 -> 3）
5) 退出
EOF
    read -r -p "请选择 [1-5]: " choice
    case "${choice}" in
      1) run_module ssh;;
      2) run_module install;;
      3) run_module config;;
      4) run_module all;;
      5) log "退出"; break;;
      *) log "无效选择：${choice}";;
    esac
  done
}

main() {
  require_root
  parse_args "$@"
  load_config
  require_master

  log "========== ${SCRIPT_NAME} START =========="
  log "conf: ${CONF_PATH}"

  if [[ -n "${RUN_MODULE}" ]]; then
    run_module "${RUN_MODULE}"
  else
    menu_loop
  fi

  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"
