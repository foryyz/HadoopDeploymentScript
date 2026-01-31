#!/usr/bin/env bash
set -euo pipefail

# Ubuntu 24 + Hadoop 3.x fully-distributed (3 nodes) installer
# Stage-2 software install & cluster config:
#   - install JDK + Hadoop (download on master, then distribute)
#   - write Hadoop xml configs on master
#   - distribute /opt (JDK/Hadoop) + env to workers
#   - establish HADOOP_USER passwordless SSH from master -> workers
#   - format NameNode (master only)
#
# IMPORTANT:
#   Run stage-1 first on all nodes:
#     sudo ./set-static-ip.sh master|worker1|worker2 --conf cluster.conf
#
# Usage:
#   sudo ./install-hadoop-ubuntu24.sh master [--conf /path/cluster.conf] [--skip-format]
#   sudo ./install-hadoop-ubuntu24.sh worker [--conf /path/cluster.conf]

ROLE="${1:-}"
shift || true
CONF_PATH="./cluster.conf"
SKIP_FORMAT=0

usage() {
  echo "Usage:" >&2
  echo "  sudo $0 master [--conf /path/to/cluster.conf] [--skip-format]" >&2
  echo "  sudo $0 worker [--conf /path/to/cluster.conf]" >&2
  exit 1
}

log()  { echo -e "[INFO] $*"; }
warn() { echo -e "[WARN] $*" >&2; }
err()  { echo -e "[ERROR] $*" >&2; exit 1; }

need_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    err "Please run as root (sudo)."
  fi
}

load_conf() {
  [[ -f "${CONF_PATH}" ]] || err "cluster.conf not found: ${CONF_PATH}"
  # shellcheck disable=SC1090
  source "${CONF_PATH}"
  [[ -n "${ADMIN_USER:-}" ]] || err "ADMIN_USER is empty in cluster.conf"
  [[ -n "${HADOOP_USER:-}" ]] || err "HADOOP_USER is empty in cluster.conf"
}

SCRIPT_PATH=""
init_paths() {
  # robust path for scp (do not depend on how the script was invoked)
  SCRIPT_PATH="$(readlink -f "$0")"
}

# parse args
[[ "${ROLE}" == "master" || "${ROLE}" == "worker" ]] || usage
while [[ $# -gt 0 ]]; do
  case "$1" in
    --conf) CONF_PATH="${2:-}"; shift 2 ;;
    --skip-format) SKIP_FORMAT=1; shift ;;
    *) err "Unknown arg: $1" ;;
  esac
done

ensure_packages() {
  log "Installing packages..."
  apt-get update -y
  apt-get install -y rsync curl wget tar net-tools openssh-client
}

ensure_hadoop_user() {
  if id "${HADOOP_USER}" >/dev/null 2>&1; then
    log "User ${HADOOP_USER} exists."
  else
    log "Creating user ${HADOOP_USER} ..."
    useradd -m -s /bin/bash "${HADOOP_USER}"
  fi

  # Hadoop scripts often call sudo (e.g., for some admin actions); keep it passwordless to avoid hangs.
  usermod -aG sudo "${HADOOP_USER}" || true
  cat > /etc/sudoers.d/99-hadoop-nopasswd <<EOF
${HADOOP_USER} ALL=(ALL) NOPASSWD:ALL
EOF
  chmod 440 /etc/sudoers.d/99-hadoop-nopasswd
}

download_and_install_jdk() {
  if [[ -d "${JDK_DIR}" && -x "${JDK_DIR}/bin/java" ]]; then
    log "JDK already installed at ${JDK_DIR}"
    return 0
  fi
  [[ -n "${JDK_DOWNLOAD_LINK:-}" ]] || err "JDK_DOWNLOAD_LINK is empty"

  log "Downloading JDK..."
  mkdir -p /tmp/hadoop_setup
  cd /tmp/hadoop_setup
  rm -f jdk8.tar.gz
  wget -O jdk8.tar.gz "${JDK_DOWNLOAD_LINK}"

  log "Installing JDK to ${JDK_DIR} ..."
  mkdir -p "${INSTALL_BASE}"
  local extract_dir="/tmp/jdk_extract_$$"
  rm -rf "${extract_dir}" && mkdir -p "${extract_dir}"
  tar -xzf jdk8.tar.gz -C "${extract_dir}"
  local top
  top="$(find "${extract_dir}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [[ -n "${top}" && -d "${top}" ]] || err "JDK extraction failed: top directory not found"

  rm -rf "${JDK_DIR}"
  mv "${top}" "${JDK_DIR}"
  rm -rf "${extract_dir}"
  [[ -x "${JDK_DIR}/bin/java" ]] || err "JDK install incomplete: ${JDK_DIR}/bin/java not executable"
  log "JDK installed OK: $(${JDK_DIR}/bin/java -version 2>&1 | head -n 1)"
}

download_and_install_hadoop() {
  if [[ -d "${HADOOP_DIR}" && -x "${HADOOP_DIR}/bin/hdfs" ]]; then
    log "Hadoop already installed at ${HADOOP_DIR}"
    return 0
  fi
  [[ -n "${HADOOP_DOWNLOAD_LINK:-}" ]] || err "HADOOP_DOWNLOAD_LINK is empty"

  log "Downloading Hadoop..."
  mkdir -p /tmp/hadoop_setup
  cd /tmp/hadoop_setup
  rm -f hadoop.tar.gz
  wget -O hadoop.tar.gz "${HADOOP_DOWNLOAD_LINK}"

  log "Installing Hadoop to ${HADOOP_DIR} ..."
  mkdir -p "${INSTALL_BASE}"
  local extract_dir="/tmp/hadoop_extract_$$"
  rm -rf "${extract_dir}" && mkdir -p "${extract_dir}"
  tar -xzf hadoop.tar.gz -C "${extract_dir}"
  local top
  top="$(find "${extract_dir}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [[ -n "${top}" && -d "${top}" ]] || err "Hadoop extraction failed: top directory not found"

  rm -rf "${HADOOP_DIR}"
  mv "${top}" "${HADOOP_DIR}"
  rm -rf "${extract_dir}"
  [[ -x "${HADOOP_DIR}/bin/hdfs" ]] || err "Hadoop install incomplete: ${HADOOP_DIR}/bin/hdfs not executable"
  log "Hadoop installed OK: $(${HADOOP_DIR}/bin/hadoop version 2>/dev/null | head -n 1 || true)"
}

write_env() {
  local env_sh="/etc/profile.d/hadoop_env.sh"
  log "Writing env to ${env_sh} ..."
  cat > "${env_sh}" <<EOF
export JAVA_HOME=${JDK_DIR}
export HADOOP_HOME=${HADOOP_DIR}
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
EOF
  chmod 644 "${env_sh}"
}

apply_java_home_everywhere() {
  log "Setting JAVA_HOME in Hadoop env files ..."
  local conf="${HADOOP_DIR}/etc/hadoop"
  for f in hadoop-env.sh yarn-env.sh mapred-env.sh; do
    local p="${conf}/${f}"
    [[ -f "${p}" ]] || continue
    if grep -qE '^export JAVA_HOME=' "${p}"; then
      sed -i "s|^export JAVA_HOME=.*|export JAVA_HOME=${JDK_DIR}|" "${p}"
    else
      echo "export JAVA_HOME=${JDK_DIR}" >> "${p}"
    fi
  done
}

make_data_dirs() {
  log "Creating data dirs under ${DATA_BASE} ..."
  mkdir -p "${DATA_BASE}/namenode" "${DATA_BASE}/datanode" "${DATA_BASE}/tmp"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${DATA_BASE}"
}

write_hadoop_configs_master() {
  log "Writing Hadoop configs on master ..."
  local conf="${HADOOP_DIR}/etc/hadoop"

  cat > "${conf}/core-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_HOST}:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>${DATA_BASE}/tmp</value>
  </property>
</configuration>
EOF

  cat > "${conf}/hdfs-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>${DFS_REPLICATION}</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:${DATA_BASE}/namenode</value>
  </property>

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:${DATA_BASE}/datanode</value>
  </property>

  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>

  <!-- SecondaryNameNode -->
  <property>
    <name>dfs.namenode.secondary.http-address</name>
    <value>${SECONDARY_HOST}:9868</value>
  </property>
</configuration>
EOF

  cat > "${conf}/mapred-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>

  <!-- JobHistoryServer on master -->
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>${MASTER_HOST}:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>${MASTER_HOST}:19888</value>
  </property>

  <!-- <=4GB VM: conservative -->
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>${MR_MAP_MB}</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>${MR_REDUCE_MB}</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx${MR_JAVA_XMX}</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx${MR_JAVA_XMX}</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>${MR_AM_MB}</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.command-opts</name>
    <value>-Xmx${MR_JAVA_XMX}</value>
  </property>
</configuration>
EOF

  cat > "${conf}/yarn-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${MASTER_HOST}</value>
  </property>

  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>

  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>${YARN_NM_MEMORY_MB}</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>${YARN_MAX_ALLOC_MB}</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>${YARN_MIN_ALLOC_MB}</value>
  </property>

  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>${YARN_NM_VCORES}</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>${YARN_NM_VCORES}</value>
  </property>
</configuration>
EOF

  # Use hostnames in workers file (more stable, matches /etc/hosts block)
  : > "${conf}/workers"
  echo "${WORKER1_HOST}" >> "${conf}/workers"
  echo "${WORKER2_HOST}" >> "${conf}/workers"

  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DIR}"
}

setup_hadoop_ssh_key_master_and_push() {
  # Ensure: master(hadoop) -> workers(hadoop) passwordless
  log "Setting up Hadoop SSH trust (master -> workers) ..."

  local h_home
  h_home="$(getent passwd "${HADOOP_USER}" | cut -d: -f6)"
  [[ -n "${h_home}" ]] || err "Cannot determine home dir for ${HADOOP_USER}"

  install -d -m 700 -o "${HADOOP_USER}" -g "${HADOOP_USER}" "${h_home}/.ssh"

  local keyfile="${h_home}/.ssh/id_ed25519"
  if [[ ! -f "${keyfile}" ]]; then
    sudo -u "${HADOOP_USER}" ssh-keygen -t ed25519 -N "" -f "${keyfile}"
  fi
  local pub
  pub="$(cat "${keyfile}.pub")"

  for ip in ${WORKERS_IPS}; do
    log "Authorizing ${HADOOP_USER} key on worker ${ip} ..."
    # We rely on ADMIN_USER passwordless sudo (stage-1) to place key into /home/${HADOOP_USER}
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${ADMIN_USER}@${ip}" \
      "sudo -u ${HADOOP_USER} mkdir -p /home/${HADOOP_USER}/.ssh && sudo -u ${HADOOP_USER} chmod 700 /home/${HADOOP_USER}/.ssh && sudo -u ${HADOOP_USER} touch /home/${HADOOP_USER}/.ssh/authorized_keys && sudo -u ${HADOOP_USER} chmod 600 /home/${HADOOP_USER}/.ssh/authorized_keys && sudo bash -c 'grep -qxF "${pub}" /home/${HADOOP_USER}/.ssh/authorized_keys || echo "${pub}" >> /home/${HADOOP_USER}/.ssh/authorized_keys'"
  done

  # Avoid hostkey prompts for the Hadoop user (optional convenience)
  local cfg="${h_home}/.ssh/config"
  if [[ ! -f "${cfg}" ]]; then
    cat > "${cfg}" <<EOF
Host *
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
EOF
    chown "${HADOOP_USER}:${HADOOP_USER}" "${cfg}"
    chmod 600 "${cfg}"
  fi
}

make_dist_tarball_master() {
  local tarball="$1"
  log "Creating distribution tarball: ${tarball}"
  tar -czf "${tarball}" -C / "${JDK_DIR#/}" "${HADOOP_DIR#/}" "etc/profile.d/hadoop_env.sh"
}

unpack_dist_worker_if_present() {
  if [[ -f /tmp/hadoop_dist.tar.gz ]]; then
    log "Unpacking distributed package ..."
    tar -xzf /tmp/hadoop_dist.tar.gz -C /
    rm -f /tmp/hadoop_dist.tar.gz
  fi
}

distribute_to_workers_master() {
  log "Distributing JDK/Hadoop/env + stage-2 script to workers via ${ADMIN_USER}@<ip> ..."

  local tarball="/tmp/hadoop_dist.tar.gz"
  make_dist_tarball_master "${tarball}"

  for ip in ${WORKERS_IPS}; do
    log "==> Sending to ${ip}"

    # Copy stage-2 script and cluster.conf into admin user's home; tarball into /tmp
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${SCRIPT_PATH}" "${ADMIN_USER}@${ip}:/home/${ADMIN_USER}/install-hadoop-ubuntu24.sh"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${CONF_PATH}" "${ADMIN_USER}@${ip}:/home/${ADMIN_USER}/cluster.conf"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${tarball}" "${ADMIN_USER}@${ip}:/tmp/hadoop_dist.tar.gz"

    # Remote run with sudo
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${ADMIN_USER}@${ip}" \
      "sudo bash /home/${ADMIN_USER}/install-hadoop-ubuntu24.sh worker --conf /home/${ADMIN_USER}/cluster.conf"
  done

  rm -f "${tarball}"
}

format_namenode_master() {
  log "Formatting NameNode (ONLY once; re-run will wipe metadata) ..."
  # shellcheck disable=SC1091
  source /etc/profile.d/hadoop_env.sh
  sudo -u "${HADOOP_USER}" "${HADOOP_DIR}/bin/hdfs" namenode -format -force
}

post_checks_master() {
  log "Sanity checks (master) ..."
  # shellcheck disable=SC1091
  source /etc/profile.d/hadoop_env.sh
  sudo -u "${HADOOP_USER}" "${HADOOP_DIR}/bin/hdfs" version | head -n 1 || true
  sudo -u "${HADOOP_USER}" "${HADOOP_DIR}/bin/yarn" version | head -n 1 || true
}

main() {
  need_root
  init_paths
  load_conf
  ensure_packages

  ensure_hadoop_user

  if [[ "${ROLE}" == "master" ]]; then
    download_and_install_jdk
    download_and_install_hadoop
    write_env
    apply_java_home_everywhere
    make_data_dirs
    write_hadoop_configs_master

    # Distribute software first, then setup hadoop ssh trust (requires worker to have HADOOP_USER)
    distribute_to_workers_master
    setup_hadoop_ssh_key_master_and_push

    if [[ "${SKIP_FORMAT}" -eq 0 ]]; then
      format_namenode_master
    else
      warn "--skip-format specified; NOT formatting NameNode."
    fi

    post_checks_master

    log "\nMaster done. Next on master:"
    log "  su - ${HADOOP_USER}"
    log "  start-dfs.sh"
    log "  start-yarn.sh"
    log "  mapred --daemon start historyserver"
    log "  jps"
    log "Web UIs:"
    log "  NameNode:        http://${MASTER_HOST}:9870"
    log "  ResourceManager: http://${MASTER_HOST}:8088"
    log "  HistoryServer:   http://${MASTER_HOST}:19888"
  else
    unpack_dist_worker_if_present

    # Fallback: if tarball wasn't provided, allow direct download
    download_and_install_jdk || true
    download_and_install_hadoop || true

    write_env
    apply_java_home_everywhere
    make_data_dirs
    chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DIR}" || true
    log "Worker done."
  fi
}

main
