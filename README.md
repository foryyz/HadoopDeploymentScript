---
title: 'Hadoop3自动部署脚本'
date: 2025-09-26T22:00:03+08:00
draft: false
Tags: ['Hadoop', 'Script']
---

# Hadoop自动化部署shell脚本

本项目用于在 **Ubuntu 24**（VMware 完整克隆的三台虚拟机）上自动部署 **3 节点完全分布式 Hadoop 集群**：

**默认集群配置：**

- `master`：NameNode / ResourceManager / JobHistoryServer
- `worker1`：DataNode / NodeManager / SecondaryNameNode
- `worker2`：DataNode / NodeManager
- `hadoop version`：3.4.2

集群安装参数、下载源、节点主机名/IP 等均可在 `cluster.conf` 中统一修改。

**Github 仓库地址：** https://github.com/foryyz/HadoopDeploymentScript



## 一 如何使用？

### 0\. 准备：安装 Ubuntu 24 & 设置 IP 段

**Ubuntu24LTS 清华镜像下载：** 
https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/noble/ubuntu-24.04.3-desktop-amd64.iso

安装系统时：

- 用户名：`hadoop`
- 主机名：`node`（可随意设置，脚本会自动改为 `cluster.conf` 中配置的主机名）
- VMware 网络段建议使用：`192.168.120.0/24`（也可自行修改）

**默认IP规划：**

- `Master`：192.168.120.10
- `Worker1`：192.168.120.11
- `Worker2`：192.168.120.12

### 1\. 在同目录创建四个文件

- `cluster.conf`
- `sc_all.sh`
- `sc_master.sh`
- `run_hadoop.sh`
### 2\. 给文件执行权限
```sh
chmod +x sc_all.sh sc_master.sh run_hadoop.sh
```
### 3\. 克隆虚拟机

关闭系统，克隆两份，作为`worker1`、`worker2`

> 提示：完整克隆后机器的 `machine-id`、SSH host key 可能重复；脚本会自动修复（可在 `cluster.conf` 中开关）。

### 4\. 按顺序执行脚本

#### 4.1 在三台机器分别执行 sc_all.sh（系统前置）

**master**

```sh
sudo ./sc_all.sh master
```
**worker1**

```sh
sudo ./sc_all.sh worker1
```
**worker2**

```sh
sudo ./sc_all.sh worker2
```
#### 4.2 在 master 执行 sc_master.sh（安装+配置+分发）

**master**

```sh
sudo ./sc_master.sh
```

### 5 启动集群

```sh
sudo ./run_hadoop.sh start
```

`run_hadoop.sh` 支持如下命令：

```sh
# 首次启动（若未格式化会自动 format）
sudo ./run_hadoop.sh start

# 只格式化（谨慎，会清空元数据）
sudo ./run_hadoop.sh format

# 只做健康检查
sudo ./run_hadoop.sh health

# 停止
sudo ./run_hadoop.sh stop

# 查看状态（jps + yarn node list）
sudo ./run_hadoop.sh status

# 重启
sudo ./run_hadoop.sh restart

# 指定配置文件
sudo ./run_hadoop.sh start --conf /path/to/cluster.conf
```

### 6\. 脚本支持参数(到第5步已经安装完了，这是关于自定义的部分)





## 二 代码

### sc_all.sh

```sh
#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# sc_all.sh - Ubuntu 24 Hadoop Cluster Prerequisites (per-node)
# - Install SSH/tools
# - Fix clone identity (machine-id + ssh host keys) (optional)
# - Set hostname
# - Configure static IP via netplan
# - Write /etc/hosts for all nodes
# - Prepare SSH keypair for HADOOP_USER
# ============================================================

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/hadoop-deploy-sc_all.log"

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

```

### sc_master.sh

```sh
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

  mkdir -p "${HADOOP_DATA_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"
  chown -R "${HADOOP_USER}:${HADOOP_USER}" "${HADOOP_DATA_DIR}" || true

  generate_hadoop_configs
  distribute_to_workers

  log "DONE: sc_master 完成安装+配置+分发"
  log "========== ${SCRIPT_NAME} DONE =========="
}

main "$@"

```

### run_hadoop.sh

```shell
#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# run_hadoop.sh - Hadoop Cluster Lifecycle (master only)
# Responsibilities:
#   - format_namenode_if_first_time
#   - start_hadoop_services / stop_hadoop_services
#   - health_check / status
# Depends on:
#   - cluster.conf
#   - sc_all.sh done on all nodes
#   - sc_master.sh done on master (install+config+distribute)
# ============================================================

SCRIPT_NAME="$(basename "$0")"
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/hadoop-deploy-run_hadoop.log"

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
  [[ -d "${HADOOP_SYMLINK}" ]] || die "找不到 HADOOP_SYMLINK=${HADOOP_SYMLINK}，请先执行 sc_master.sh"
  [[ -x "${JAVA_DIR}/bin/java" ]] || die "找不到 JAVA=${JAVA_DIR}/bin/java，请先执行 sc_master.sh"

  # hosts resolvable
  local h
  for h in "${CLUSTER_HOSTNAMES[@]}"; do
    getent hosts "${h}" >/dev/null 2>&1 || die "无法解析 ${h}，请确认三台已跑 sc_all.sh 并写好 /etc/hosts"
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

```

### cluster.conf

```apache
# =========================
# 基础用户配置
# =========================
HADOOP_USER="hadoop"
HADOOP_GROUP="hadoop"
TIMEZONE="Asia/Shanghai"


# =========================
# 网络配置（VMware）
# =========================
NET_PREFIX="192.168.120"
NET_CIDR="24"
GATEWAY="192.168.120.2"
DNS_SERVERS=("114.114.114.114" "8.8.8.8")


# =========================
# IP 规划（默认）
# sc_all.sh 不传 --ip 时会读取这里的默认值
# =========================
DEFAULT_IP_MASTER="192.168.120.10"
DEFAULT_IP_WORKER1="192.168.120.11"
DEFAULT_IP_WORKER2="192.168.120.12"


# =========================
# 主机名规划（可随时修改）
# =========================
MASTER_HOSTNAME="master"
WORKER1_HOSTNAME="worker1"
WORKER2_HOSTNAME="worker2"


# =========================
# 集群角色规划
# =========================
ENABLE_SECONDARY_NAMENODE="true"
SECONDARY_NAMENODE_HOSTNAME="${WORKER1_HOSTNAME}"

ENABLE_JOBHISTORYSERVER="true"
JOBHISTORYSERVER_HOSTNAME="${MASTER_HOSTNAME}"


# =========================
# Hadoop / JDK 下载源
# =========================
JDK_DOWNLOAD_LINK="https://mirrors.tuna.tsinghua.edu.cn/Adoptium/8/jdk/x64/linux/OpenJDK8U-jdk_x64_linux_hotspot_8u472b08.tar.gz"
HADOOP_DOWNLOAD_LINK="https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz"


# =========================
# 安装路径规划
# =========================
INSTALL_BASE="/opt"
JAVA_DIR="${INSTALL_BASE}/jdk8"
HADOOP_DIR="${INSTALL_BASE}/hadoop"
HADOOP_SYMLINK="${INSTALL_BASE}/hadoop-current"


# =========================
# Hadoop 数据目录
# =========================
HADOOP_DATA_DIR="/data/hadoop"
HDFS_NAME_DIR="${HADOOP_DATA_DIR}/namenode"
HDFS_DATA_DIR="${HADOOP_DATA_DIR}/datanode"


# =========================
# Hadoop 核心参数
# =========================
FS_DEFAULT_PORT="9000"
HDFS_REPLICATION="2"


# =========================
# MapReduce JobHistoryServer
# =========================
MAPREDUCE_JOBHISTORY_ADDRESS_PORT="10020"
MAPREDUCE_JOBHISTORY_WEBAPP_PORT="19888"


# =========================
# SSH / 分发策略（hadoop@worker）
# =========================
# copy-id : 执行时手动输入 worker 上 hadoop 的密码（推荐）
# sshpass : 全自动（教学环境）
SSH_PUSH_MODE="copy-id"

# 仅当 SSH_PUSH_MODE=sshpass 时需要
SSH_DEFAULT_PASSWORD="hadoop"


# =========================
# VMware 克隆修复
# =========================
FIX_CLONE_IDENTITY="true"


# =========================
# Netplan / Ubuntu 网络
# =========================
NETPLAN_RENDERER="networkd"


# =========================
# 集群节点清单（自动生成 hosts / workers）
# =========================
CLUSTER_HOSTNAMES=(
  "${MASTER_HOSTNAME}"
  "${WORKER1_HOSTNAME}"
  "${WORKER2_HOSTNAME}"
)

CLUSTER_IPS=(
  "${DEFAULT_IP_MASTER}"
  "${DEFAULT_IP_WORKER1}"
  "${DEFAULT_IP_WORKER2}"
)

```

## 三 脚本支持参数说明

### sc_all.sh

#### 用法与示例

```sh
sudo ./sc_all.sh <master|worker1|worker2> [options]

# 示例
# 使用 cluster.conf 默认 IP
sudo ./sc_all.sh worker1

# 覆盖 IP
sudo ./sc_all.sh worker1 --ip 192.168.120.88

# 覆盖网关/DNS
sudo ./sc_all.sh master --gateway 192.168.120.2 --dns 114.114.114.114,8.8.8.8
```

#### options

- `--conf <path>`
   指定配置文件路径（默认：脚本同目录 `cluster.conf`）
- `--ip <IPv4>`
   覆盖该节点默认 IP（默认不需要写；不写则按角色读取 `cluster.conf`）
  - master → `DEFAULT_IP_MASTER`
  - worker1 → `DEFAULT_IP_WORKER1`
  - worker2 → `DEFAULT_IP_WORKER2`
- `--hostname <name>`
   覆盖该节点主机名（默认按 `cluster.conf` 的 `MASTER_HOSTNAME/WORKER1_HOSTNAME/WORKER2_HOSTNAME`）
- `--cidr <n>`
   覆盖 `NET_CIDR`
- `--gateway <IPv4>`
   覆盖 `GATEWAY`
- `--dns <a,b,c>`
   覆盖 DNS（逗号分隔，例如 `--dns 114.114.114.114,8.8.8.8`）

### sc_master.sh

#### 用法

```sh
sudo ./sc_master.sh [options]
```

#### options

- `--conf <path>`
   指定配置文件路径（默认：脚本同目录 `cluster.conf`）
- `--force`
   强制重新安装/覆盖 JDK & Hadoop（谨慎使用）

### run_hadoop.sh

### 用法

```sh
sudo ./run_hadoop.sh <start|stop|restart|status|format|health> [--conf <path>]
```

#### options

- `--conf <path>`
   指定配置文件路径（默认：脚本同目录 `cluster.conf`）

## 四 脚本任务说明

`sc_all.sh` 主要完成：

- 安装基础依赖（ssh/rsync 等）
- 修复克隆导致的冲突（machine-id、ssh host keys）
- 配置主机名
- 配置静态 IP（默认读取 `cluster.conf`，也可参数覆盖）
- 写入 `/etc/hosts`
- 为 `hadoop` 用户生成 SSH key，并配置 **self-ssh（自己免密登录自己）**，避免 Hadoop 启动时 NameNode 失败

`sc_master.sh` 主要完成：

- 在 master 下载并安装 JDK / Hadoop
- 生成 Hadoop 集群配置
- 将 JDK/Hadoop/配置通过 `hadoop@worker` 分发到 worker1/worker2，并使用 sudo 落地到 `/opt`、`/etc/profile.d`、`/data`

> 注意：`SSH_PUSH_MODE=copy-id` 时，会提示你输入 worker 上 `hadoop` 用户密码（用于 ssh-copy-id），以及远程 sudo 密码（通常也是 hadoop 密码）。
>  若希望全自动，可在 `cluster.conf` 设置 `SSH_PUSH_MODE=sshpass` 并填写 `SSH_DEFAULT_PASSWORD`。



## Q Web UI 访问地址是什么？

**默认端口**（Hadoop 3.x 常见）：

- NameNode UI：`http://master:9870`
- ResourceManager UI：`http://master:8088`
- JobHistory UI：`http://master:19888`



## x 参考文献

2020年, [hadoop环境部署自动化shell脚本（伪分布式、完全分布式集群搭建）](https://blog.csdn.net/weixin_42011520/article/details/107571965)

2018年, [使用Shell脚本一键部署Hadoop](https://blog.csdn.net/u010993514/article/details/83349846)
