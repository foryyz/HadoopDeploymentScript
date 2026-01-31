#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# sc_3.sh - Hadoop Cluster Lifecycle (master only)
# Responsibilities:
#   - format_namenode_if_first_time
#   - start_hadoop_services / stop_hadoop_services
#   - health_check / status
# Depends on:
#   - cluster.conf
#   - sc_1.sh done on all nodes
#   - sc_2.sh done on master (install+config+distribute)
# ============================================================

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/hadoop-deploy-sc_3.log"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE" >&2; }
die() { log "ERROR: $*"; exit 1; }

trap 'ec=$?; log "ERROR(exit=$ec) line ${BASH_LINENO[0]}: ${BASH_COMMAND}"; exit $ec' ERR

CONF_PATH=""
ACTION="${1:-}"
shift || true

usage() {
  cat >&2 <<EOF
用法:
  sudo ./${SCRIPT_NAME} <start|stop|restart|status|format|health> [--conf /path/cluster.conf]

说明:
  - 仅在 master 执行
  - start: 如未格式化则自动 format，然后启动 dfs+yarn+historyserver(可选)
  - format: 强制格式化 NameNode（危险）
  - health: 集群健康检查
EOF
}

require_root() {
  [[ "${EUID}" -eq 0 ]] || die "请用 root 或 sudo 运行：sudo ./${SCRIPT_NAME} ..."
}

parse_args() {
  if [[ -z "${ACTION}" ]]; then
    usage; exit 1
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --conf) CONF_PATH="$2"; shift 2;;
      -h|--help) usage; exit 0;;
      *) die "未知参数: $1";;
    esac
  done

  case "${ACTION}" in
    start|stop|restart|status|format|health) ;;
    *) die "未知动作: ${ACTION}（支持 start/stop/restart/status/format/health）";;
  esac
}

load_config() {
  if [[ -z "${CONF_PATH}" ]]; then
    if [[ -f "${WORKDIR}/cluster.conf" ]]; then
      CONF_PATH="${WORKDIR}/cluster.conf"
    else
      die "找不到 cluster.conf，请用 --conf 指定"
    fi
  fi

  # shellcheck disable=SC1090
  source "${CONF_PATH}"

  : "${HADOOP_USER:?}"
  : "${MASTER_HOSTNAME:?}"
  : "${WORKER1_HOSTNAME:?}"
  : "${WORKER2_HOSTNAME:?}"
  : "${CLUSTER_HOSTNAMES:?}"
  : "${HADOOP_SYMLINK:?}"
  : "${JAVA_DIR:?}"
  : "${HADOOP_DATA_DIR:?}"
  : "${HDFS_NAME_DIR:?}"
  : "${HDFS_DATA_DIR:?}"

  : "${ENABLE_JOBHISTORYSERVER:=false}"
  : "${JOBHISTORYSERVER_HOSTNAME:=}"
}

require_master() {
  local hn
  hn="$(hostnamectl --static 2>/dev/null || hostname)"
  [[ "${hn}" == "${MASTER_HOSTNAME}" ]] || die "只能在 master 执行：当前=${hn} 期望=${MASTER_HOSTNAME}"
}

hadoop_env() {
  export JAVA_HOME="${JAVA_DIR}"
  export HADOOP_HOME="${HADOOP_SYMLINK}"
  export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${JAVA_HOME}/bin:${PATH}"
}

as_hadoop() {
  # run command as hadoop user with env loaded
  sudo -u "${HADOOP_USER}" -H bash -lc "export JAVA_HOME='${JAVA_DIR}'; export HADOOP_HOME='${HADOOP_SYMLINK}'; export PATH=\"\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$JAVA_HOME/bin:\$PATH\"; $*"
}

assert_prerequisites() {
  [[ -d "${HADOOP_SYMLINK}" ]] || die "找不到 HADOOP_SYMLINK=${HADOOP_SYMLINK}，请先执行 sc_2.sh"
  [[ -x "${JAVA_DIR}/bin/java" ]] || die "找不到 JAVA=${JAVA_DIR}/bin/java，请先执行 sc_2.sh"

  # hosts resolvable
  local h
  for h in "${CLUSTER_HOSTNAMES[@]}"; do
    getent hosts "${h}" >/dev/null 2>&1 || die "无法解析 ${h}，请确认三台已跑 sc_1.sh 并写好 /etc/hosts"
  done

  # data dirs
  mkdir -p "${HADOOP_DATA_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DATA_DIR}" || true
}

# ------------------------------------------------------------
# 1) format_namenode_if_first_time
# ------------------------------------------------------------
is_namenode_formatted() {
  # Hadoop creates VERSION file in current dir when formatted:
  # ${HDFS_NAME_DIR}/current/VERSION
  [[ -f "${HDFS_NAME_DIR}/current/VERSION" ]]
}

format_namenode_if_first_time() {
  if is_namenode_formatted; then
    log "NameNode 已格式化（发现 ${HDFS_NAME_DIR}/current/VERSION），跳过 format。"
    return 0
  fi

  log "检测到 NameNode 未格式化，开始执行 hdfs namenode -format ..."
  # ensure name dir exists & owned
  mkdir -p "${HDFS_NAME_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HDFS_NAME_DIR}" || true

  # non-interactive format: -force -nonInteractive
  as_hadoop "hdfs namenode -format -force -nonInteractive"
  log "NameNode format 完成。"
}

format_namenode_force() {
  log "警告：你正在执行强制 format，将清空 NameNode 元数据：${HDFS_NAME_DIR}"
  as_hadoop "hdfs namenode -format -force -nonInteractive"
  log "强制 format 完成。"
}

# ------------------------------------------------------------
# 2) start_hadoop_services / stop
# ------------------------------------------------------------
start_hadoop_services() {
  log "启动 HDFS（start-dfs.sh）..."
  as_hadoop "start-dfs.sh"

  log "启动 YARN（start-yarn.sh）..."
  as_hadoop "start-yarn.sh"

  if [[ "${ENABLE_JOBHISTORYSERVER}" == "true" ]]; then
    # historyserver can be started on any host, we start on master by design
    log "启动 JobHistoryServer（mapred --daemon start historyserver）..."
    as_hadoop "mapred --daemon start historyserver"
  else
    log "ENABLE_JOBHISTORYSERVER!=true，跳过 JobHistoryServer。"
  fi

  log "启动命令已执行完成（不代表所有进程都已就绪，health 可验证）。"
}

stop_hadoop_services() {
  if [[ "${ENABLE_JOBHISTORYSERVER}" == "true" ]]; then
    log "停止 JobHistoryServer ..."
    as_hadoop "mapred --daemon stop historyserver" || true
  fi

  log "停止 YARN（stop-yarn.sh）..."
  as_hadoop "stop-yarn.sh" || true

  log "停止 HDFS（stop-dfs.sh）..."
  as_hadoop "stop-dfs.sh" || true

  log "停止命令已执行完成。"
}

# ------------------------------------------------------------
# 3) health_check / status
# ------------------------------------------------------------
status_local_jps() {
  log "本机 jps："
  as_hadoop "jps" || true
}

status_workers_jps() {
  log "worker jps（通过 hadoop 用户 ssh）:"
  local w
  for w in "${WORKER1_HOSTNAME}" "${WORKER2_HOSTNAME}"; do
    log "---- ${w} ----"
    # do not fail whole script if one worker unreachable
    as_hadoop "ssh -o BatchMode=yes -o ConnectTimeout=5 ${HADOOP_USER}@${w} 'jps || true'" || true
  done
}

health_hdfs_basic() {
  log "HDFS 基础检查：创建并列出 /tmp ..."
  as_hadoop "hdfs dfs -mkdir -p /tmp" || true
  as_hadoop "hdfs dfs -ls /" || true
}

health_yarn_nodes() {
  log "YARN NodeManager 列表："
  as_hadoop "yarn node -list" || true
}

health_check() {
  log "========== HEALTH CHECK =========="
  status_local_jps
  status_workers_jps
  health_hdfs_basic
  health_yarn_nodes
  log "========== END HEALTH CHECK ======"
}

status() {
  log "========== STATUS =========="
  status_local_jps
  health_yarn_nodes
  log "提示：更完整请用 health"
  log "========== END STATUS ====="
}

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
main() {
  require_root
  parse_args "$@"
  load_config
  require_master
  assert_prerequisites
  hadoop_env

  log "========== ${SCRIPT_NAME} START =========="
  log "action: ${ACTION}, conf: ${CONF_PATH}"

  case "${ACTION}" in
    start)
      format_namenode_if_first_time
      start_hadoop_services
      ;;
    stop)
      stop_hadoop_services
      ;;
    restart)
      stop_hadoop_services
      format_namenode_if_first_time
      start_hadoop_services
      ;;
    status)
      status
      ;;
    format)
      format_namenode_force
      ;;
    health)
      health_check
      ;;
  esac

  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"
