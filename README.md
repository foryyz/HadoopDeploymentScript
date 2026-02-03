---
title: 'Hadoop&Spark 自动部署脚本'
date: 2025-09-26T22:00:03+08:00
draft: false
Tags: ['Hadoop', 'Spark', 'Script']
---

> Writing Time - 2025/09/26
>
> Last Update Time - 2026/02/03

---

本项目用于在 **Ubuntu 24**（VMware 完整克隆的三台虚拟机）上**自动部署** **3 节点完全分布式 Hadoop 集群**和**安装与启动Spark**，支持**SparkSQL**和**PySpark** (Spark On Yarn)

**默认集群配置：**

- `master`：NameNode / ResourceManager / JobHistoryServer
- `worker1`：DataNode / NodeManager / SecondaryNameNode
- `worker2`：DataNode / NodeManager
- `hadoop version`：3.4.2
- `spark version`：3.5.8

**默认IP规划：**

- `Master`：192.168.120.10
- `Worker1`：192.168.120.11
- `Worker2`：192.168.120.12

集群安装参数、下载源、节点主机名/IP 等均可在 `cluster.conf` 中统一修改。

**Github 仓库地址：** https://github.com/foryyz/HadoopDeploymentScript



## 一 如何使用？

### 0\. 准备：安装 Ubuntu 24 & 设置 IP 段

**Ubuntu24LTS 清华镜像下载：** 
https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/noble/ubuntu-24.04.3-desktop-amd64.iso

安装系统时：

- 用户名：`hadoop`
- 主机名：`node`（**可随意设置**，脚本会自动改为 `cluster.conf` 中配置的主机名）
- VMware 网络段建议使用：`192.168.120.0/24`（也可自行修改）

### 1\. 下载脚本文件

可从[Github仓库](https://github.com/foryyz/HadoopDeploymentScript)下载，也可以直接从下方的代码直接复制

- `cluster.conf`
- `sc_all.sh`
- `sc_master.sh`
- `run_hadoop.sh`
- `spark_install.sh`
- `spark_run.sh`
#### 给予文件执行权限
```sh
chmod +x sc_all.sh sc_master.sh run_hadoop.sh spark_install.sh spark_run.sh
```
### 2\. 克隆虚拟机

关闭系统，克隆两份，作为`worker1`、`worker2`

> 提示：完整克隆后机器的 `machine-id`、SSH host key 可能重复；脚本会自动修复（可在 `cluster.conf` 中开关）。

### 3\. 安装hadoop

#### 3.1 在三台机器分别执行 sc_all.sh

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
#### 3.2 在 master 执行 sc_master.sh（安装+配置+分发）

**master**

```sh
sudo ./sc_master.sh
```

**master、worker1、worker2** 检查是否真正安装成功了jdk和hadoop

```
java -version
hadoop version
```

如果**输出不成功重启系统或者刷新环境变量**即可

```sh
# 刷新环境变量
source /etc/profile.d/java.sh /etc/profile.d/hadoop.sh
```

#### 3\.3 启动hadoop集群

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

### 4\. 安装Spark

以下所有命令都在**master**主机执行

#### 4\.1 执行安装脚本

```shell
sudo ./spark_install.sh
```

**环境变量**地址`/etc/profile.d/spark.sh`

启动成功后可以访问web地址: http://master:18080

`spark_run.sh` 支持如下命令：

```shell
sudo ./spark_run.sh install
sudo ./spark_run.sh start
sudo ./spark_run.sh stop
sudo ./spark_run.sh status
sudo ./spark_run.sh restart
sudo ./spark_run.sh health
sudo ./spark_run.sh env
sudo ./spark_run.sh shell
```

#### 4\.2 启动SparkSQL & PySpark

```shell
# 启动SparkSQL
sudo ./spark_run.sh sparksql
# 启动PySpark
sudo ./spark_run.sh pyspark
```

`metastore_db` 目录：`/data/spark/metastore_db`

## 二 脚本支持参数说明

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

- `--module <name>`
   - `ssh` 只执行ssh免密
   - `install`  只安装和分发jdk hadoop
   - `config` 分发hadoop的xml配置文件
   - `all`  执行全部流程
   

### run_hadoop.sh

#### 用法

```sh
sudo ./run_hadoop.sh <start|stop|restart|status|format|health|env|shell> [--conf <path>]
```

#### options

- `--conf <path>`
   指定配置文件路径（默认：脚本同目录 `cluster.conf`）

### spark_install.sh

#### 用法

```shell
sudo ./spark_install.sh <start|stop|restart|status|health|sparksql|pyspark> [--conf <path>] [--force]
```

#### options

- `--conf <path>`
   指定集群配置文件路径
   （默认：脚本同目录下的 `cluster.conf`）
- `--force`
   强制重新安装 Spark
  - 会删除已有的 Spark 安装目录并重新解压
  - 适用于版本切换或配置异常修复
  - ⚠️ 谨慎使用（会覆盖现有 Spark 安装）

#### commands

- `install`
   下载安装Spark
- `start`
  启动 Spark History Server
  - 若 HDFS 已启动且 `SPARK_EVENTLOG_DIR_HDFS` 不存在，会自动创建
  - History Server 默认监听端口：`18080`
- `stop`
   停止 Spark History Server
- `restart`
   重启 Spark History Server
- `status`
   查看 Spark History Server 状态
  - 显示 `jps` 输出
  - 检查 History Server 端口监听情况
- `health`
   Spark 运行健康检查
  - 检查 HDFS eventlog 目录是否可访问
  - 检查 History Server 进程与端口状态
- `sparksql`
   启动 Spark SQL（Spark on YARN）
  - 自动加载 Java / Hadoop / Spark 环境
  - 使用 YARN 作为资源调度
  - 使用固定的 Spark SQL metastore 目录
  - 退出方式：`exit` 或 `Ctrl+D`
- `pyspark`
   启动 PySpark（Spark on YARN）
  - 自动加载环境变量
  - 可与 `sparksql` 同时运行
  - 退出方式：`exit` 或 `Ctrl+D`



## 三 脚本任务说明

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

`spark_install.sh` 主要完成：

- 下载 Spark 安装包
- 安装 Spark 到 `/opt`
- 配置 Spark on YARN
- 配置 Spark SQL / History Server
- 创建 eventlog / warehouse 目录
- 分发 Spark 到所有 worker
- 刷新 master 环境变量

## Q Web UI 访问地址是什么？

**默认端口**（Hadoop 3.x 常见）：

- NameNode UI：`http://master:9870`
- ResourceManager UI：`http://master:8088`
- JobHistory UI：`http://master:19888`



## x 参考文献

2020年, [hadoop环境部署自动化shell脚本（伪分布式、完全分布式集群搭建）](https://blog.csdn.net/weixin_42011520/article/details/107571965)

2018年, [使用Shell脚本一键部署Hadoop](https://blog.csdn.net/u010993514/article/details/83349846)
