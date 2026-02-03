#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/spark-deploy.log"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE" >&2; }
die() { log "ERROR: $*"; exit 1; }

trap 'ec=$?; log "ERROR(exit=$ec) line ${BASH_LINENO[0]}: ${BASH_COMMAND}"; exit $ec' ERR

CONF_PATH=""
FORCE_REINSTALL="false"
ACTION=""

usage() {
  cat >&2 <<EOF
Usage:
  sudo ./${SCRIPT_NAME} [install|start|stop|restart|status|health|env|shell|sparksql|pyspark]
  sudo ./${SCRIPT_NAME} [--conf /path/cluster.conf] [--force]

Notes:
  - Run on master only
  - Spark install/config/distribute + Spark History Server management
  - If no action is provided, a numeric menu is shown

Options:
  --conf <path>   Specify config file (default: ${WORKDIR}/cluster.conf)
  --force         Force reinstall (only for install)
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
      --action) ACTION="$2"; shift 2;;
      *)
        if [[ -z "${ACTION}" ]]; then
          ACTION="$1"
          shift 1
        else
          die "未知参数: $1"
        fi
        ;;
    esac
  done

  if [[ -n "${ACTION}" ]]; then
    case "${ACTION}" in
      install|start|stop|restart|status|health|env|shell|sparksql|pyspark) ;;
      *) die "未知动作: ${ACTION}（支持 install/start/stop/restart/status/health/env/shell/sparksql/pyspark）";;
    esac
  fi
}

show_menu_and_pick() {
  cat >&2 <<EOF
请选择操作：
  1) 安装/配置/分发 Spark
  2) 启动 History Server
  3) 停止 History Server
  4) 重启 History Server
  5) 状态查看
  6) 健康检查
  7) 输出环境刷新命令
  8) 进入 Spark Shell
  9) 启动 Spark SQL
 10) 启动 PySpark
  0) 退出
EOF
  read -r -p "输入数字: " choice
  case "${choice}" in
    1) ACTION="install";;
    2) ACTION="start";;
    3) ACTION="stop";;
    4) ACTION="restart";;
    5) ACTION="status";;
    6) ACTION="health";;
    7) ACTION="env";;
    8) ACTION="shell";;
    9) ACTION="sparksql";;
    10) ACTION="pyspark";;
    0|q|Q) exit 0;;
    *) die "无效选择: ${choice}";;
  esac
}

apt_install() {
  local pkgs=($@)
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

  # Spark run helpers
  : "${SPARK_LOCAL_METASTORE_DIR:?}"
}

require_master() {
  local hn
  hn="$(hostnamectl --static 2>/dev/null || hostname)"
  [[ "${hn}" == "${MASTER_HOSTNAME}" ]] || die "只能在 master 执行：当前 ${hn} 期望=${MASTER_HOSTNAME}"
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
      log "ssh-copy-id ${HADOOP_USER}@${w}（需输入 ${HADOOP_USER} 密码）"
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

  command -v spark-submit >/dev/null 2>&1 && log "spark-submit 可用：$(spark-submit --version 2>/dev/null | head -n1)" || \
    log "提示：当前 shell 未检测到 spark-submit（新登录后一定生效）"
}

# ---- Generate Spark configs for Yarn ----
ensure_spark_conf_dir() {
  local conf_dir="${SPARK_SYMLINK}/conf"
  [[ -d "${conf_dir}" ]] || die "找不到 Spark conf 目录：${conf_dir}"
}

backup_if_needed() {
  local f="$1"
  if [[ -f "${f}" ]]; then
    if ! grep -q "Generated by spark" "${f}"; then
      local bak="${f}.bak.$(date '+%F_%H%M%S')"
      cp -a "${f}" "${bak}"
      log "已备份 ${f} -> ${bak}"
    fi
  fi
}

write_spark_env_sh() {
  local conf_dir="${SPARK_SYMLINK}/conf"
  local f="${conf_dir}/spark-env.sh"

  backup_if_needed "${f}"

  cat > "${f}" <<EOF
#!/usr/bin/env bash
# Generated by ${SCRIPT_NAME}

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

  backup_if_needed "${f}"

  # Spark on YARN + Spark SQL + EventLog + HistoryServer
  cat > "${f}" <<EOF
# Generated by ${SCRIPT_NAME}
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
# Minimal log4j2 config (generated by spark_install.sh)
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

# ---- Run/Manage Spark ----
print_env_refresh_cmd() {
  log "在当前终端刷新环境变量，请执行："
  echo "source /etc/profile.d/java.sh && source /etc/profile.d/hadoop.sh && source /etc/profile.d/spark.sh"
}

open_spark_shell() {
  log "进入已加载 Spark 环境的交互 Shell（退出输入 exit）..."
  bash -lc "source /etc/profile.d/java.sh 2>/dev/null || true;
           source /etc/profile.d/hadoop.sh 2>/dev/null || true;
           source /etc/profile.d/spark.sh 2>/dev/null || true;
           exec bash"
}

ensure_enabled() {
  if [[ "${ENABLE_SPARK_HISTORY_SERVER}" != "true" ]]; then
    die "ENABLE_SPARK_HISTORY_SERVER!=true，当前配置未启用 History Server。"
  fi
}

ensure_spark_installed() {
  [[ -x "${SPARK_SYMLINK}/sbin/start-history-server.sh" ]] || die "未发现 Spark：${SPARK_SYMLINK}，请先执行 install"
}

ensure_hdfs_eventlog_dir() {
  # 1) 检查 HDFS 是否可用（最稳的方法：hdfs dfs -ls /）
  if ! as_hadoop "hdfs dfs -ls / >/dev/null 2>&1"; then
    log "提示：HDFS 似乎未启动或不可用，跳过创建 ${SPARK_EVENTLOG_DIR_HDFS}"
    log "      你可以先执行：sudo ./sc_3.sh start"
    return 0
  fi

  # 2) 检查 eventlog 目录是否存在
  if as_hadoop "hdfs dfs -test -d '${SPARK_EVENTLOG_DIR_HDFS}'"; then
    log "HDFS eventlog 目录已存在：${SPARK_EVENTLOG_DIR_HDFS}"
    return 0
  fi

  # 3) 不存在则创建
  log "检测到 HDFS eventlog 目录不存在，开始创建：${SPARK_EVENTLOG_DIR_HDFS}"
  as_hadoop "hdfs dfs -mkdir -p '${SPARK_EVENTLOG_DIR_HDFS}'"
  # 给写入权限（多应用写 eventlog 更省事）
  as_hadoop "hdfs dfs -chmod 1777 '${SPARK_EVENTLOG_DIR_HDFS}'" || true
  log "已创建 HDFS eventlog 目录：${SPARK_EVENTLOG_DIR_HDFS}"
}

start_history() {
  ensure_enabled
  ensure_spark_installed

  if [[ "${SPARK_HISTORYSERVER_HOSTNAME}" != "${MASTER_HOSTNAME}" ]]; then
    die "当前脚本只支持 History Server 部署在 master。配置为 ${SPARK_HISTORYSERVER_HOSTNAME}"
  fi

  # start 前确认 HDFS eventlog 目录存在（HDFS 未启动则跳过）
  ensure_hdfs_eventlog_dir

  log "启动 Spark History Server..."
  as_hadoop "${SPARK_SYMLINK}/sbin/start-history-server.sh"
  log "启动命令已执行。"
}

stop_history() {
  ensure_enabled
  ensure_spark_installed

  log "停止 Spark History Server..."
  as_hadoop "${SPARK_SYMLINK}/sbin/stop-history-server.sh" || true
  log "停止命令已执行。"
}

status() {
  log "jps（master）："
  as_hadoop "jps" || true

  log "端口监听检查（${SPARK_HISTORYSERVER_UI_PORT}）："
  ss -lntp 2>/dev/null | awk -v p=":${SPARK_HISTORYSERVER_UI_PORT}" '$4 ~ p {print $0}' || true
}

health() {
  ensure_enabled
  ensure_spark_installed

  log "========== HEALTH CHECK =========="

  log "检查 HDFS eventlog 目录可读：${SPARK_EVENTLOG_DIR_HDFS}"
  as_hadoop "hdfs dfs -ls '${SPARK_EVENTLOG_DIR_HDFS}'" || log "提示：无法访问 eventlog 目录（HDFS 未启动或目录不存在）"

  log "检查 HistoryServer 进程与端口："
  status

  log "========== END HEALTH CHECK ======"
}

run_spark_sql() {
  ensure_spark_installed

  : "${SPARK_LOCAL_METASTORE_DIR:=/data/spark/metastore_db}"

  log "启动 Spark SQL（Spark on YARN）..."
  log "退出请使用 Ctrl+D 或 exit"

  # 创建固定目录（避免 metastore_db 出现在桌面）
  mkdir -p "${SPARK_LOCAL_METASTORE_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "$(dirname "${SPARK_LOCAL_METASTORE_DIR}")" || true

  exec sudo -u "${HADOOP_USER}" -H bash -lc "
    export JAVA_HOME='${JAVA_DIR}';
    export HADOOP_HOME='${HADOOP_SYMLINK}';
    export HADOOP_CONF_DIR=\"\$HADOOP_HOME/etc/hadoop\";
    export YARN_CONF_DIR=\"\$HADOOP_CONF_DIR\";
    export SPARK_HOME='${SPARK_SYMLINK}';
    export PATH=\"\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$HADOOP_HOME/bin:\$JAVA_HOME/bin:\$PATH\";

    # 确保从固定目录启动（防止生成 ./metastore_db）
    cd '$(dirname "${SPARK_LOCAL_METASTORE_DIR}")';

    exec spark-sql --master yarn \
      --conf spark.sql.catalogImplementation=hive \
      --conf 'spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:${SPARK_LOCAL_METASTORE_DIR};create=true'
  "
}

run_pyspark() {
  ensure_spark_installed

  log "启动 PySpark（Spark on YARN）..."
  log "退出请使用 Ctrl+D 或 exit"

  exec sudo -u "${HADOOP_USER}" -H bash -lc "
    export JAVA_HOME='${JAVA_DIR}';
    export HADOOP_HOME='${HADOOP_SYMLINK}';
    export HADOOP_CONF_DIR=\"\$HADOOP_HOME/etc/hadoop\";
    export YARN_CONF_DIR=\"\$HADOOP_CONF_DIR\";
    export SPARK_HOME='${SPARK_SYMLINK}';
    export PATH=\"\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$HADOOP_HOME/bin:\$JAVA_HOME/bin:\$PATH\";
    exec pyspark --master yarn
  "
}

run_install_flow() {
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
}

main() {
  require_root
  parse_args "$@"
  load_config
  require_master

  if [[ -z "${ACTION}" ]]; then
    show_menu_and_pick
  fi

  log "========== ${SCRIPT_NAME} START =========="
  log "action: ${ACTION}, conf: ${CONF_PATH}"

  case "${ACTION}" in
    install) run_install_flow ;;
    start) start_history ;;
    stop) stop_history ;;
    restart) stop_history; start_history ;;
    status) status ;;
    health) health ;;
    env) print_env_refresh_cmd ;;
    shell) open_spark_shell ;;
    sparksql) run_spark_sql ;;
    pyspark) run_pyspark ;;
  esac

  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"