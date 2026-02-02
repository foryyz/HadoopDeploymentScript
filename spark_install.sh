#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/spark-deploy-spark_2.log"

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
  - 负责 Spark 下载/安装/配置，并通过 hadoop@worker 分发到 workers
  - Spark 部署模式：Spark on YARN
  - 不负责启动服务（History Server 由 spark_3.sh 负责）

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

  # Cluster basics
  : "${HADOOP_USER:?}"
  : "${MASTER_HOSTNAME:?}"
  : "${WORKER1_HOSTNAME:?}"
  : "${WORKER2_HOSTNAME:?}"
  : "${CLUSTER_HOSTNAMES:?}"

  # Hadoop install (used for HDFS create checks)
  : "${HADOOP_SYMLINK:?}"

  # Java (Spark needs Java)
  : "${JAVA_DIR:?}"

  # SSH push strategy
  : "${SSH_PUSH_MODE:?}"        # copy-id | sshpass
  : "${SSH_DEFAULT_PASSWORD:?}" # only for sshpass mode

  # Spark config (new fields)
  : "${SPARK_DOWNLOAD_LINK:?}"
  : "${SPARK_DIR:?}"
  : "${SPARK_SYMLINK:?}"
  : "${SPARK_MASTER:?}"
  : "${SPARK_DEPLOY_MODE_DEFAULT:?}"
  : "${ENABLE_SPARK_EVENTLOG:?}"
  : "${SPARK_EVENTLOG_DIR_LOCAL:?}"
  : "${SPARK_EVENTLOG_DIR_HDFS:?}"
  : "${ENABLE_SPARK_HISTORY_SERVER:?}"
  : "${SPARK_HISTORYSERVER_HOSTNAME:?}"
  : "${SPARK_HISTORYSERVER_UI_PORT:?}"
  : "${SPARK_SQL_WAREHOUSE_DIR:?}"
}

require_master() {
  local hn
  hn="$(hostnamectl --static 2>/dev/null || hostname)"
  [[ "${hn}" == "${MASTER_HOSTNAME}" ]] || die "只能在 master 执行：当前=${hn} 期望=${MASTER_HOSTNAME}"
}

get_workers() { echo "${WORKER1_HOSTNAME} ${WORKER2_HOSTNAME}"; }

assert_hosts_ready() {
  local h
  for h in "${CLUSTER_HOSTNAMES[@]}"; do
    getent hosts "${h}" >/dev/null 2>&1 || die "无法解析 ${h}，请确认三台已跑 sc_1.sh 并写好 /etc/hosts"
  done
}

# Run as hadoop user with login shell, but inject JAVA/SPARK/HADOOP env for safety
as_hadoop() {
  sudo -u "${HADOOP_USER}" -H bash -lc \
    "export JAVA_HOME='${JAVA_DIR}'; export PATH=\"\$JAVA_HOME/bin:\$PATH\"; $*"
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
  [[ -f "${uhome}/.ssh/id_rsa.pub" ]] || die "master 上 ${HADOOP_USER} 未生成 SSH key，请先跑 sc_1.sh"

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

remote_sudo() {
  local host="$1"; shift
  local cmd="$*"

  if [[ "${SSH_PUSH_MODE}" == "copy-id" ]]; then
    # interactive: may ask for sudo password on worker
    sudo -u "${HADOOP_USER}" ssh -tt "${HADOOP_USER}@${host}" "sudo bash -lc $(printf '%q' "${cmd}")"
  else
    apt_install sshpass >/dev/null
    sudo -u "${HADOOP_USER}" sshpass -p "${SSH_DEFAULT_PASSWORD}" ssh -tt "${HADOOP_USER}@${host}" \
      "echo '${SSH_DEFAULT_PASSWORD}' | sudo -S bash -lc $(printf '%q' "${cmd}")"
  fi
}

# ---- Download & install Spark on master ----
fname() { echo "${1##*/}"; }

download_spark() {
  mkdir -p "/opt/src"
  local tar="/opt/src/$(fname "${SPARK_DOWNLOAD_LINK}")"

  if [[ ! -f "${tar}" ]]; then
    log "下载 Spark: ${SPARK_DOWNLOAD_LINK}"
    curl -L --fail -o "${tar}" "${SPARK_DOWNLOAD_LINK}"
  else
    log "Spark 包已存在：${tar}"
  fi

  tar -tzf "${tar}" >/dev/null
  echo "${tar}"
}

install_spark_local() {
  local spark_tar="$1"

  mkdir -p "/opt/.tmp_spark"
  rm -rf "/opt/.tmp_spark/*" || true
  tar -xzf "${spark_tar}" -C "/opt/.tmp_spark"

  local top
  top="$(find "/opt/.tmp_spark" -mindepth 1 -maxdepth 1 -type d | head -n1)"
  [[ -n "${top}" ]] || die "Spark 包结构异常：${spark_tar}"

  # e.g. spark-3.5.8-bin-hadoop3
  local version_dir="${SPARK_DIR}-$(basename "${top}" | sed 's/^spark-//')"

  if [[ -d "${version_dir}" && "${FORCE_REINSTALL}" != "true" ]]; then
    log "Spark 已存在且未 --force，跳过覆盖：${version_dir}"
    rm -rf "/opt/.tmp_spark"
  else
    log "安装 Spark 到 ${version_dir}"
    rm -rf "${version_dir}"
    mv "${top}" "${version_dir}"
    rm -rf "/opt/.tmp_spark"
  fi

  rm -f "${SPARK_SYMLINK}"
  ln -s "${version_dir}" "${SPARK_SYMLINK}"

  # profile.d
  cat > /etc/profile.d/spark.sh <<EOF
export SPARK_HOME="${SPARK_SYMLINK}"
export PATH="\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH"
EOF
  chmod 644 /etc/profile.d/spark.sh

  # Ensure readable, and let hadoop user manage logs/conf if needed
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${version_dir}" || true

  log "Spark 安装完成：${SPARK_SYMLINK} -> ${version_dir}"
}

refresh_env_local() {
  log "刷新本机环境变量（JAVA/HADOOP/SPARK）..."

  # shellcheck disable=SC1091
  source /etc/profile.d/java.sh || true
  # shellcheck disable=SC1091
  source /etc/profile.d/hadoop.sh || true
  # shellcheck disable=SC1091
  source /etc/profile.d/spark.sh || true

  command -v spark-submit >/dev/null 2>&1 && log "spark-submit 已可用：$(spark-submit --version 2>/dev/null | head -n1)" || \
    log "提示：当前 shell 未检测到 spark-submit（新登录后一定生效）"
}

# ---- Generate Spark configs for Yarn ----
ensure_spark_conf_dir() {
  local conf_dir="${SPARK_SYMLINK}/conf"
  [[ -d "${conf_dir}" ]] || die "找不到 Spark conf 目录：${conf_dir}"
}

write_spark_env_sh() {
  local conf_dir="${SPARK_SYMLINK}/conf"
  local f="${conf_dir}/spark-env.sh"

  cat > "${f}" <<EOF
#!/usr/bin/env bash
# Generated by spark_2.sh

export JAVA_HOME="${JAVA_DIR}"
export HADOOP_CONF_DIR="${HADOOP_SYMLINK}/etc/hadoop"
export YARN_CONF_DIR="\${HADOOP_CONF_DIR}"

# Keep logs in a stable path
export SPARK_LOG_DIR="${SPARK_EVENTLOG_DIR_LOCAL}"
EOF
  chmod 755 "${f}"
}

write_spark_defaults() {
  local conf_dir="${SPARK_SYMLINK}/conf"
  local f="${conf_dir}/spark-defaults.conf"

  # Spark on YARN + Spark SQL + EventLog + HistoryServer
  cat > "${f}" <<EOF
# Generated by spark_2.sh
spark.master                          ${SPARK_MASTER}
spark.submit.deployMode               ${SPARK_DEPLOY_MODE_DEFAULT}

# Spark SQL
spark.sql.warehouse.dir               ${SPARK_SQL_WAREHOUSE_DIR}

# Event log (recommended)
spark.eventLog.enabled                ${ENABLE_SPARK_EVENTLOG}
spark.eventLog.dir                    ${SPARK_EVENTLOG_DIR_HDFS}

# History Server reads this directory
spark.history.fs.logDirectory         ${SPARK_EVENTLOG_DIR_HDFS}
spark.history.ui.port                 ${SPARK_HISTORYSERVER_UI_PORT}

# YARN: use HADOOP_USERName to avoid permission surprise on HDFS
spark.yarn.appMasterEnv.HADOOP_USER_NAME ${HADOOP_USER}
spark.executorEnv.HADOOP_USER_NAME    ${HADOOP_USER}

# (Optional tuning examples - uncomment if needed)
# spark.executor.instances            2
# spark.executor.cores                1
# spark.executor.memory               1g
# spark.driver.memory                 1g
EOF
}

write_spark_log4j2_optional() {
  # Spark 3.5 uses log4j2; we keep a minimal config to reduce console spam.
  local conf_dir="${SPARK_SYMLINK}/conf"
  local f="${conf_dir}/log4j2.properties"

  if [[ -f "${f}" ]]; then
    # don't overwrite if user customized
    return 0
  fi

  cat > "${f}" <<'EOF'
# Minimal log4j2 config (generated by spark_2.sh)
status = error
name = SparkLog4j2

appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

rootLogger.level = info
rootLogger.appenderRefs = console
rootLogger.appenderRef.console.ref = console
EOF
}

ensure_local_eventlog_dir_master() {
  mkdir -p "${SPARK_EVENTLOG_DIR_LOCAL}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "$(dirname "${SPARK_EVENTLOG_DIR_LOCAL}")" || true
}

try_create_hdfs_dirs() {
  # HDFS must be running. If not, we warn and continue.
  if [[ ! -x "${HADOOP_SYMLINK}/bin/hdfs" ]]; then
    log "未检测到 hdfs 命令（HADOOP_SYMLINK=${HADOOP_SYMLINK}），跳过创建 HDFS 目录。"
    return 0
  fi

  # If HDFS not started, these will fail; we do not stop the script.
  log "尝试创建 HDFS 目录：${SPARK_EVENTLOG_DIR_HDFS} 和 ${SPARK_SQL_WAREHOUSE_DIR}"
  as_hadoop "hdfs dfs -mkdir -p '${SPARK_EVENTLOG_DIR_HDFS}'" || log "提示：创建 ${SPARK_EVENTLOG_DIR_HDFS} 失败（可能 HDFS 未启动），可在 sc_3.sh start 后重试"
  as_hadoop "hdfs dfs -mkdir -p '${SPARK_SQL_WAREHOUSE_DIR}'" || log "提示：创建 ${SPARK_SQL_WAREHOUSE_DIR} 失败（可能 HDFS 未启动），可在 sc_3.sh start 后重试"

  # permissions (optional)
  as_hadoop "hdfs dfs -chmod 1777 '${SPARK_EVENTLOG_DIR_HDFS}'" || true
  as_hadoop "hdfs dfs -chmod 1777 '${SPARK_SQL_WAREHOUSE_DIR}'" || true
}

# ---- Distribute Spark to workers ----
distribute_to_workers() {
  local version_dir
  version_dir="$(readlink -f "${SPARK_SYMLINK}")"

  local w
  for w in $(get_workers); do
    log "=== 分发 Spark 到 ${w}（hadoop@worker + sudo 落地）==="

    # stage dir under home
    sudo -u "${HADOOP_USER}" ssh "${HADOOP_USER}@${w}" "mkdir -p /home/${HADOOP_USER}/.stage_spark" >/dev/null

    log "rsync Spark -> ${w} 临时目录"
    sudo -u "${HADOOP_USER}" rsync -az --delete "${version_dir}/" "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_spark/spark/"

    log "rsync /etc/profile.d/spark.sh -> ${w} 临时目录"
    sudo -u "${HADOOP_USER}" rsync -az /etc/profile.d/spark.sh "${HADOOP_USER}@${w}:/home/${HADOOP_USER}/.stage_spark/spark.sh"

    log "落地到系统目录（sudo）"
    remote_sudo "${w}" "
      mkdir -p '${version_dir}' '${SPARK_DIR}' '${SPARK_EVENTLOG_DIR_LOCAL}';
      rm -rf '${version_dir}';
      mkdir -p '${version_dir}';
      rsync -a --delete /home/${HADOOP_USER}/.stage_spark/spark/ '${version_dir}/';

      rm -f '${SPARK_SYMLINK}';
      ln -s '${version_dir}' '${SPARK_SYMLINK}';

      mv /home/${HADOOP_USER}/.stage_spark/spark.sh /etc/profile.d/spark.sh;
      chmod 644 /etc/profile.d/spark.sh;

      id -u '${HADOOP_USER}' >/dev/null 2>&1 || useradd -m -s /bin/bash '${HADOOP_USER}';
      chown -R '${HADOOP_USER}:${HADOOP_USER}' '${version_dir}' '${SPARK_EVENTLOG_DIR_LOCAL}';

      # validate profile.d for login shell
      bash -lc 'source /etc/profile.d/spark.sh; command -v spark-submit >/dev/null' || true
    "

    log "${w} Spark 分发完成。"
  done
}

main() {
  require_root
  parse_args "$@"
  load_config
  require_master

  log "========== ${SCRIPT_NAME} START =========="
  log "conf: ${CONF_PATH}"

  # Tools needed
  apt_install openssh-client rsync curl wget tar ca-certificates openssh-server
  systemctl enable --now ssh >/dev/null 2>&1 || true

  assert_hosts_ready

  prepare_hadoop_known_hosts
  push_hadoop_key_to_workers

  local spark_tar
  spark_tar="$(download_spark)"

  install_spark_local "${spark_tar}"

  ensure_spark_conf_dir
  write_spark_env_sh
  write_spark_defaults
  write_spark_log4j2_optional

  ensure_local_eventlog_dir_master
  try_create_hdfs_dirs

  # Refresh local env so user doesn't need reboot/source
  refresh_env_local

  distribute_to_workers

  log "DONE: spark_2 完成 Spark 安装+配置+分发（Spark on YARN）"
  log "下一步：若启用 History Server，运行：sudo ./spark_3.sh start"
  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"
