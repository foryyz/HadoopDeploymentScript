#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/spark-deploy-spark_run.log"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE" >&2; }
die() { log "ERROR: $*"; exit 1; }

trap 'ec=$?; log "ERROR(exit=$ec) line ${BASH_LINENO[0]}: ${BASH_COMMAND}"; exit $ec' ERR

CONF_PATH=""
ACTION="${1:-}"
shift || true

usage() {
  cat >&2 <<EOF
用法:
  sudo ./${SCRIPT_NAME} <start|stop|restart|status|health|env|shell> [--conf /path/cluster.conf]

说明:
  - 仅在 master 上执行
  - start/stop/restart/status/health : 管理 Spark History Server
  - env/shell       ：输出刷新终端的命令
  - sparksql        : 启动 Spark SQL（Spark on YARN）
  - pyspark         : 启动 PySpark（Spark on YARN）
EOF
}

require_root() {
  [[ "${EUID}" -eq 0 ]] || die "请用 root 或 sudo 运行：sudo ./${SCRIPT_NAME} ..."
}

parse_args() {
  [[ -n "${ACTION}" ]] || { usage; exit 1; }

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --conf) CONF_PATH="$2"; shift 2;;
      -h|--help) usage; exit 0;;
      *) die "未知参数: $1";;
    esac
  done

  case "${ACTION}" in
    start|stop|restart|status|health|env|shell|sparksql|pyspark) ;;
    *) die "未知动作: ${ACTION}（支持 start/stop/restart/status/health/env/shell/sparksql/pyspark）";;
  esac
}

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

load_config() {
  if [[ -z "${CONF_PATH}" ]]; then
    [[ -f "${WORKDIR}/cluster.conf" ]] || die "找不到 cluster.conf，请用 --conf 指定"
    CONF_PATH="${WORKDIR}/cluster.conf"
  fi
  # shellcheck disable=SC1090
  source "${CONF_PATH}"

  : "${HADOOP_USER:?}"
  : "${MASTER_HOSTNAME:?}"
  : "${JAVA_DIR:?}"
  : "${HADOOP_SYMLINK:?}"

  : "${SPARK_SYMLINK:?}"
  : "${ENABLE_SPARK_HISTORY_SERVER:?}"
  : "${SPARK_HISTORYSERVER_HOSTNAME:?}"
  : "${SPARK_HISTORYSERVER_UI_PORT:?}"
  : "${SPARK_EVENTLOG_DIR_HDFS:?}"
}

require_master() {
  local hn
  hn="$(hostnamectl --static 2>/dev/null || hostname)"
  [[ "${hn}" == "${MASTER_HOSTNAME}" ]] || die "只能在 master 执行：当前=${hn} 期望=${MASTER_HOSTNAME}"
}

as_hadoop() {
  sudo -u "${HADOOP_USER}" -H bash -lc "
    export JAVA_HOME='${JAVA_DIR}';
    export HADOOP_HOME='${HADOOP_SYMLINK}';
    export HADOOP_CONF_DIR=\"\$HADOOP_HOME/etc/hadoop\";
    export YARN_CONF_DIR=\"\$HADOOP_CONF_DIR\";
    export SPARK_HOME='${SPARK_SYMLINK}';
    export PATH=\"\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$HADOOP_HOME/bin:\$JAVA_HOME/bin:\$PATH\";
    $*
  "
}

ensure_enabled() {
  if [[ "${ENABLE_SPARK_HISTORY_SERVER}" != "true" ]]; then
    die "ENABLE_SPARK_HISTORY_SERVER!=true，当前配置未启用 History Server。"
  fi
}

ensure_spark_installed() {
  [[ -x "${SPARK_SYMLINK}/sbin/start-history-server.sh" ]] || die "未发现 Spark：${SPARK_SYMLINK}，请先运行 spark_install.sh"
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
    die "当前脚本只支持 History Server 部署在 master。配置=${SPARK_HISTORYSERVER_HOSTNAME}"
  fi

# start 前确保 HDFS eventlog 目录存在（HDFS 未启动则跳过）
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
  log "jps（master）:"
  as_hadoop "jps" || true

  log "端口监听检查（${SPARK_HISTORYSERVER_UI_PORT}）:"
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

main() {
  require_root
  parse_args "$@"
  load_config
  require_master

  log "========== ${SCRIPT_NAME} START =========="
  log "action: ${ACTION}, conf: ${CONF_PATH}"

  case "${ACTION}" in
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
