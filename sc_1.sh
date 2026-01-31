#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# sc_1.sh - Ubuntu 24 Hadoop Cluster Prerequisites (per-node)
# - Install SSH/tools
# - Fix clone identity (machine-id + ssh host keys) (optional)
# - Set hostname
# - Configure static IP via netplan
# - Write /etc/hosts for all nodes
# - Prepare SSH keypair for HADOOP_USER
# ============================================================

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/hadoop-deploy-sc_1.log"

# --------------------- Logging ---------------------
log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE" >&2; }
die() { log "ERROR: $*"; exit 1; }

on_err() {
  local ec=$?
  log "ERROR: command failed (exit=$ec) at line ${BASH_LINENO[0]}: ${BASH_COMMAND}"
  exit "$ec"
}
trap on_err ERR

# --------------------- Defaults (can be overridden by cluster.conf) ---------------------
CONF_PATH=""
ROLE=""
STATIC_IP=""
OVERRIDE_HOSTNAME=""
OVERRIDE_NET_PREFIX=""
OVERRIDE_NET_CIDR=""
OVERRIDE_GATEWAY=""
OVERRIDE_DNS=""

resolve_static_ip_by_role() {
  if [[ -n "${STATIC_IP}" ]]; then
    # 命令行 --ip 优先
    return 0
  fi

  case "${ROLE}" in
    master)
      STATIC_IP="${DEFAULT_IP_MASTER}"
      ;;
    worker1)
      STATIC_IP="${DEFAULT_IP_WORKER1}"
      ;;
    worker2)
      STATIC_IP="${DEFAULT_IP_WORKER2}"
      ;;
    *)
      die "无法为未知角色分配 IP：${ROLE}"
      ;;
  esac

  log "未指定 --ip，使用 cluster.conf 中的默认 IP：${STATIC_IP}"
}

# --------------------- Helpers ---------------------
require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    die "请用 root 或 sudo 运行：sudo ./${SCRIPT_NAME} ..."
  fi
}

command_exists() { command -v "$1" >/dev/null 2>&1; }

trim() {
  local s="$1"
  # shellcheck disable=SC2001
  echo "$(echo "$s" | sed 's/^[[:space:]]\+//; s/[[:space:]]\+$//')"
}

# Convert "a,b,c" => array
split_csv_to_array() {
  local csv="$1"
  local -n out_arr="$2"
  IFS=',' read -r -a out_arr <<< "$csv"
}

# --------------------- Config ---------------------
load_config() {
  # default: cluster.conf in script dir
  if [[ -z "${CONF_PATH}" ]]; then
    if [[ -f "${WORKDIR}/cluster.conf" ]]; then
      CONF_PATH="${WORKDIR}/cluster.conf"
    else
      die "找不到配置文件 cluster.conf。请使用 --conf 指定路径，或把 cluster.conf 放到脚本同目录。"
    fi
  fi

  # shellcheck disable=SC1090
  source "${CONF_PATH}"

  # Basic required fields
  : "${HADOOP_USER:?cluster.conf 缺少 HADOOP_USER}"
  : "${MASTER_HOSTNAME:?cluster.conf 缺少 MASTER_HOSTNAME}"
  : "${WORKER1_HOSTNAME:?cluster.conf 缺少 WORKER1_HOSTNAME}"
  : "${WORKER2_HOSTNAME:?cluster.conf 缺少 WORKER2_HOSTNAME}"
  : "${DEFAULT_IP_MASTER:?cluster.conf 缺少 DEFAULT_IP_MASTER}"
  : "${DEFAULT_IP_WORKER1:?cluster.conf 缺少 DEFAULT_IP_WORKER1}"
  : "${DEFAULT_IP_WORKER2:?cluster.conf 缺少 DEFAULT_IP_WORKER2}"
  : "${NET_CIDR:?cluster.conf 缺少 NET_CIDR}"
  : "${GATEWAY:?cluster.conf 缺少 GATEWAY}"
  : "${NETPLAN_RENDERER:?cluster.conf 缺少 NETPLAN_RENDERER}"
  : "${FIX_CLONE_IDENTITY:?cluster.conf 缺少 FIX_CLONE_IDENTITY}"
  : "${CLUSTER_HOSTNAMES:?cluster.conf 缺少 CLUSTER_HOSTNAMES}"
  : "${CLUSTER_IPS:?cluster.conf 缺少 CLUSTER_IPS}"
}

# --------------------- Args ---------------------
usage() {
  cat >&2 <<EOF
用法:
  sudo ./${SCRIPT_NAME} <master|worker1|worker2> --ip <IPv4> [options]

必选:
  role                   节点角色: master / worker1 / worker2
  --ip <IPv4>            本机静态IP

可选:
  --conf <path>          配置文件路径 (默认: 脚本同目录 cluster.conf)
  --hostname <name>      覆盖配置文件中的主机名（不建议，除非你确实要改）
  --cidr <n>             覆盖 NET_CIDR（默认来自 cluster.conf）
  --gateway <IPv4>       覆盖网关（默认来自 cluster.conf）
  --dns <a,b,c>          覆盖 DNS_SERVERS（默认来自 cluster.conf）
  -h, --help             帮助

示例:
  sudo ./${SCRIPT_NAME} master  --ip 192.168.120.10
  sudo ./${SCRIPT_NAME} worker1 --ip 192.168.120.11 --conf /path/cluster.conf
EOF
}

parse_args() {
  if [[ $# -lt 1 ]]; then usage; exit 1; fi

  ROLE="$(trim "$1")"
  shift || true

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --conf) CONF_PATH="$2"; shift 2;;
      --ip) STATIC_IP="$2"; shift 2;;
      --hostname) OVERRIDE_HOSTNAME="$2"; shift 2;;
      --cidr) OVERRIDE_NET_CIDR="$2"; shift 2;;
      --gateway) OVERRIDE_GATEWAY="$2"; shift 2;;
      --dns) OVERRIDE_DNS="$2"; shift 2;;
      -h|--help) usage; exit 0;;
      *) die "未知参数: $1 (用 -h 查看帮助)";;
    esac
  done

  case "${ROLE}" in
    master|worker1|worker2) ;;
    *) die "role 必须是 master/worker1/worker2，你输入的是: ${ROLE}";;
  esac

}

validate_ipv4() {
  local ip="$1"
  if [[ ! "$ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
    return 1
  fi
  # check each octet 0-255
  IFS='.' read -r o1 o2 o3 o4 <<< "$ip"
  for o in "$o1" "$o2" "$o3" "$o4"; do
    [[ "$o" -ge 0 && "$o" -le 255 ]] || return 1
  done
  return 0
}

# --------------------- NIC detect ---------------------
detect_primary_nic() {
  # Use default route interface
  local nic
  nic="$(ip route | awk '/default/ {print $5; exit}')"
  [[ -n "${nic}" ]] || die "无法检测默认路由网卡。请确认网络已连接。"
  echo "${nic}"
}

# --------------------- Package install ---------------------
apt_install() {
  local pkgs=("$@")
  export DEBIAN_FRONTEND=noninteractive

  log "apt update..."
  apt-get update -y >/dev/null

  log "apt install: ${pkgs[*]} ..."
  apt-get install -y "${pkgs[@]}" >/dev/null
}

install_base_packages() {
  local pkgs=(openssh-server openssh-client rsync curl wget tar net-tools)
  apt_install "${pkgs[@]}"

  systemctl enable --now ssh >/dev/null 2>&1 || true
  systemctl restart ssh >/dev/null 2>&1 || true
  log "SSH 服务已启用。"
}

# --------------------- Fix clone identity ---------------------
fix_clone_identity() {
  # VMware 完整克隆很可能复制 machine-id 与 ssh host keys
  if [[ "${FIX_CLONE_IDENTITY}" != "true" ]]; then
    log "FIX_CLONE_IDENTITY=false，跳过克隆身份修复。"
    return 0
  fi

  log "修复克隆身份：重建 machine-id + 重新生成 SSH host keys ..."

  # machine-id
  if [[ -f /etc/machine-id ]]; then
    rm -f /etc/machine-id
  fi
  if [[ -f /var/lib/dbus/machine-id ]]; then
    rm -f /var/lib/dbus/machine-id
  fi
  systemd-machine-id-setup >/dev/null

  # ssh host keys
  rm -f /etc/ssh/ssh_host_* || true
  dpkg-reconfigure openssh-server >/dev/null 2>&1 || true
  ssh-keygen -A >/dev/null 2>&1 || true
  systemctl restart ssh >/dev/null 2>&1 || true

  log "克隆身份修复完成。"
}

# --------------------- Hostname ---------------------
desired_hostname_for_role() {
  case "${ROLE}" in
    master) echo "${MASTER_HOSTNAME}" ;;
    worker1) echo "${WORKER1_HOSTNAME}" ;;
    worker2) echo "${WORKER2_HOSTNAME}" ;;
  esac
}

set_hostname() {
  local desired
  if [[ -n "${OVERRIDE_HOSTNAME}" ]]; then
    desired="${OVERRIDE_HOSTNAME}"
  else
    desired="$(desired_hostname_for_role)"
  fi

  local current
  current="$(hostnamectl --static 2>/dev/null || hostname)"

  if [[ "${current}" == "${desired}" ]]; then
    log "主机名已是 ${desired}，跳过。"
    return 0
  fi

  log "设置主机名: ${current} -> ${desired}"
  hostnamectl set-hostname "${desired}"
}

# --------------------- Netplan ---------------------
configure_netplan() {
  local nic="$1"
  local cidr="${NET_CIDR}"
  local gw="${GATEWAY}"
  local -a dns_servers=("${DNS_SERVERS[@]}")

  if [[ -n "${OVERRIDE_NET_CIDR}" ]]; then cidr="${OVERRIDE_NET_CIDR}"; fi
  if [[ -n "${OVERRIDE_GATEWAY}" ]]; then gw="${OVERRIDE_GATEWAY}"; fi
  if [[ -n "${OVERRIDE_DNS}" ]]; then
    dns_servers=()
    split_csv_to_array "${OVERRIDE_DNS}" dns_servers
  fi

  validate_ipv4 "${STATIC_IP}" || die "--ip 不是合法 IPv4：${STATIC_IP}"
  validate_ipv4 "${gw}" || die "GATEWAY 不是合法 IPv4：${gw}"

  for d in "${dns_servers[@]}"; do
    d="$(trim "$d")"
    [[ -n "$d" ]] || continue
    validate_ipv4 "$d" || die "DNS 不是合法 IPv4：$d"
  done

  local file="/etc/netplan/01-hadoop-cluster.yaml"

  log "写入 netplan: ${file} (nic=${nic}, ip=${STATIC_IP}/${cidr})"

  cat > "${file}" <<EOF
network:
  version: 2
  renderer: ${NETPLAN_RENDERER}
  ethernets:
    ${nic}:
      dhcp4: no
      addresses:
        - ${STATIC_IP}/${cidr}
      routes:
        - to: default
          via: ${gw}
      nameservers:
        addresses: [$(printf '%s,' "${dns_servers[@]}" | sed 's/,$//')]
EOF

  chmod 600 "${file}"

  log "netplan apply..."
  netplan apply

  # quick verify
  local newip
  newip="$(ip -4 addr show "${nic}" | awk '/inet / {print $2}' | head -n1 || true)"
  log "当前网卡 ${nic} IPv4: ${newip:-unknown}"
}

# --------------------- /etc/hosts ---------------------
write_etc_hosts() {
  # Create an idempotent block
  local hosts_file="/etc/hosts"
  local begin="# BEGIN HADOOP_CLUSTER_HOSTS"
  local end="# END HADOOP_CLUSTER_HOSTS"

  log "更新 ${hosts_file}（幂等区块）..."

  # Build block content from arrays
  local block="${begin}\n"
  local i
  for i in "${!CLUSTER_HOSTNAMES[@]}"; do
    local hn="${CLUSTER_HOSTNAMES[$i]}"
    local ip="${CLUSTER_IPS[$i]}"
    block+="${ip}\t${hn}\n"
  done
  block+="${end}\n"

  # Remove old block if exists
  if grep -qF "${begin}" "${hosts_file}"; then
    # delete from begin to end inclusive
    sed -i "/${begin}/,/${end}/d" "${hosts_file}"
  fi

  # Ensure localhost lines exist (do not remove existing; just ensure common ones)
  if ! grep -qE '^127\.0\.0\.1\s+localhost' "${hosts_file}"; then
    echo -e "127.0.0.1\tlocalhost" >> "${hosts_file}"
  fi
  if ! grep -qE '^::1\s+localhost' "${hosts_file}"; then
    echo -e "::1\tlocalhost ip6-localhost ip6-loopback" >> "${hosts_file}"
  fi

  # Append block
  echo -e "${block}" >> "${hosts_file}"

  log "/etc/hosts 已写入集群映射："
  echo -e "${block}" | tee -a "$LOG_FILE" >/dev/null
}

# --------------------- SSH keys for HADOOP_USER ---------------------
ensure_hadoop_user_exists() {
  if id -u "${HADOOP_USER}" >/dev/null 2>&1; then
    return 0
  fi

  log "用户 ${HADOOP_USER} 不存在，创建中..."
  useradd -m -s /bin/bash "${HADOOP_USER}"
  # 这里不设置密码（教学环境可自行设置），后续免密通过 ssh-copy-id 推送
  log "已创建用户 ${HADOOP_USER}。"
}

prepare_ssh_keys_for_user() {
  ensure_hadoop_user_exists

  local uhome
  uhome="$(eval echo "~${HADOOP_USER}")"
  local ssh_dir="${uhome}/.ssh"
  local key_file="${ssh_dir}/id_rsa"

  log "为用户 ${HADOOP_USER} 准备 SSH 密钥..."

  mkdir -p "${ssh_dir}"
  chown "${HADOOP_USER}:${HADOOP_USER}" "${ssh_dir}"
  chmod 700 "${ssh_dir}"

  if [[ ! -f "${key_file}" ]]; then
    sudo -u "${HADOOP_USER}" ssh-keygen -t rsa -b 4096 -N "" -f "${key_file}" >/dev/null
    log "已生成 ${key_file}"
  else
    log "密钥已存在，跳过生成：${key_file}"
  fi

  # ensure authorized_keys exists with proper perms
  touch "${ssh_dir}/authorized_keys"
  chown "${HADOOP_USER}:${HADOOP_USER}" "${ssh_dir}/authorized_keys"
  chmod 600 "${ssh_dir}/authorized_keys"
}
ensure_self_ssh_trust_for_user() {
  local u="${HADOOP_USER}"
  local uhome
  uhome="$(eval echo "~${u}")"

  log "为用户 ${u} 配置本机免密 SSH（self-ssh）..."

  # 1) known_hosts 加入本机 hostname（避免 StrictHostKeyChecking 卡住）
  sudo -u "${u}" bash -lc "ssh-keyscan -H \"$(hostnamectl --static 2>/dev/null || hostname)\" 2>/dev/null >> \"${uhome}/.ssh/known_hosts\" || true"
  sudo -u "${u}" bash -lc "ssh-keyscan -H 127.0.0.1 2>/dev/null >> \"${uhome}/.ssh/known_hosts\" || true"
  sudo -u "${u}" bash -lc "ssh-keyscan -H localhost 2>/dev/null >> \"${uhome}/.ssh/known_hosts\" || true"

  # 2) authorized_keys 包含自己的公钥（关键）
  sudo -u "${u}" bash -lc "cat \"${uhome}/.ssh/id_rsa.pub\" >> \"${uhome}/.ssh/authorized_keys\""
  sudo -u "${u}" bash -lc "sort -u \"${uhome}/.ssh/authorized_keys\" -o \"${uhome}/.ssh/authorized_keys\""
  chmod 600 "${uhome}/.ssh/authorized_keys"
}

# --------------------- Checks ---------------------
system_checks_summary() {
  local nic
  nic="$(detect_primary_nic)"

  log "========== SUMMARY =========="
  log "role            : ${ROLE}"
  log "hostname        : $(hostnamectl --static 2>/dev/null || hostname)"
  log "primary nic     : ${nic}"
  log "expected ip     : ${STATIC_IP}/${NET_CIDR}"
  log "current ip      : $(ip -4 addr show "${nic}" | awk '/inet / {print $2}' | head -n1 || echo 'unknown')"
  log "ssh status      : $(systemctl is-active ssh 2>/dev/null || echo 'unknown')"
  log "hosts resolve   :"
  for h in "${CLUSTER_HOSTNAMES[@]}"; do
    log "  - ${h} => $(getent hosts "${h}" | awk '{print $1}' | head -n1 || echo 'unresolved')"
  done
  log "log file        : ${LOG_FILE}"
  log "============================="
}

# --------------------- Main ---------------------
main() {
  require_root
  parse_args "$@"
  load_config

  # 在这里自动确定 IP
  resolve_static_ip_by_role
  
  # role -> if user didn't set cluster.conf CLUSTER_IPS to match, still okay:
  # /etc/hosts will reflect cluster.conf. The local static IP uses --ip.
  # If you want cluster.conf to track overrides, user can edit cluster.conf manually.

  log "========== ${SCRIPT_NAME} START =========="
  log "使用配置文件: ${CONF_PATH}"
  log "角色: ${ROLE}, 设置静态IP: ${STATIC_IP}"

  install_base_packages
  fix_clone_identity
  set_hostname

  local nic
  nic="$(detect_primary_nic)"
  configure_netplan "${nic}"

  write_etc_hosts
  prepare_ssh_keys_for_user
  ensure_self_ssh_trust_for_user

  system_checks_summary
  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"
