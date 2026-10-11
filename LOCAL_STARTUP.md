# 本地启动 Uniffle 服务并查看 Dashboard

本文档基于实测环境整理(macOS aarch64 / JDK 1.8 / Maven 3.9.6),已在本机验证 build 成功并可启动。

## 0. 前置条件

| 项 | 要求 | 本机情况 |
|---|---|---|
| JDK | 1.8(build 要求) | jdk-1.8 |
| Maven | 3.x(仓库自带 `./mvnw`) | 3.9.6 |
| 内存 | 建议空闲 4G+ | — |

确认 JAVA_HOME:
```bash
echo $JAVA_HOME
# 若为空,设为本机 jdk8 路径
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home
```

## 1. 构建(trap:公司 Maven 镜像缺一个 Apache 孵化期包)

直接 `./mvnw clean package` 会失败,根因:
- pom 的 `maven-remote-resources-plugin` 硬编码了 `org.apache.apache.resources:apache-incubator-disclaimer-resource-bundle:1.2-SNAPSHOT`;
- 公司镜像 `mirror-all`(odin.shizhuang-inc.com)没有这个 artifact;
- 它只在 Apache 官方 SNAPSHOT 仓库(repository.apache.org)有。

**解决:从 Apache 仓库手动装到本地 m2(一次性)。**

```bash
cd /tmp
BASE="https://repository.apache.org/content/repositories/snapshots/org/apache/apache/resources/apache-incubator-disclaimer-resource-bundle/1.2-SNAPSHOT"
REAL=$(curl -s "$BASE/" | grep -oE 'apache-incubator-disclaimer-resource-bundle-1\.2-[0-9]{8}\.[0-9]{6}-[0-9]+\.jar' | sort -u | tail -1)
curl -s -o "$REAL" "$BASE/$REAL"

cd /Users/admin/Workspaces/uniffle-master
rm -rf ~/.m2/repository/org/apache/apache/resources/apache-incubator-disclaimer-resource-bundle/1.2-SNAPSHOT/*
./mvnw install:install-file \
  -DgroupId=org.apache.apache.resources \
  -DartifactId=apache-incubator-disclaimer-resource-bundle \
  -Dversion=1.2-SNAPSHOT -Dpackaging=jar \
  -Dfile="/tmp/$REAL"
```

**然后 build coordinator / server / dashboard 三个模块**(`-Pdashboard` 激活 dashboard profile,`-am` 连带构建依赖模块):

```bash
cd /Users/admin/Workspaces/uniffle-master
./mvnw -DskipTests clean package -Pdashboard -pl coordinator,server,dashboard -am
```

> 首次会下载 node v20.15.0 + npm 依赖并构建 Vue 前端,耗时 5–15 分钟。看到 `BUILD SUCCESS` 即成功。

## 2. 配置本地启动参数

仓库自带本地开发模板 `dev/local_dev_template/`,端口已规划好:

| 组件 | RPC 端口 | HTTP 端口 |
|---|---|---|
| Coordinator | 19999 | 19998 |
| Shuffle Server | 19979 | 19978 |
| Dashboard | — | 19997 |

把模板拷到 `conf/`:
```bash
cp dev/local_dev_template/coordinator.conf conf/coordinator.conf
cp dev/local_dev_template/dashboard.conf   conf/dashboard.conf
cp dev/local_dev_template/server.conf      conf/server.conf
cp dev/local_dev_template/log4j2.xml       conf/log4j2.xml
```

### 2.1 改 server.conf(本地无 HDFS,且内存要调小)

编辑 `conf/server.conf`,关键改动:
```
rss.storage.basePath /tmp/rss-dir0,/tmp/rss-dir1
rss.storage.type MEMORY_LOCALFILE          # 原 MEMORY_LOCALFILE_HDFS 需 Hadoop 集群,本地去掉 HDFS
rss.coordinator.quorum localhost:19999      # 指向本机 coordinator
rss.server.buffer.capacity 512mb            # 原 40gb,本地调小避免 OOM
rss.server.read.buffer.capacity 256mb       # 原 20gb
rss.server.disk.capacity 1g
```
其余保留默认。

### 2.2 改 rss-env.sh(JAVA_HOME + 调小堆)

编辑 `conf/rss-env.sh`,把开头的内存段改成:
```bash
JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home}"
XMX_SIZE="2g"                 # Coordinator/Dashboard 堆(原 8g)
SHUFFLE_SERVER_XMX_SIZE="2g"   # ShuffleServer 堆(原 80g)
```

## 3. 组装 jars 目录(启动脚本从这里读 jar)

启动脚本 `bin/start-*.sh` 从 `${RSS_HOME}/jars/{coordinator,server,dashboard}/*.jar` 读 classpath,build 只产在 `*/target`,需手动组装(等价 `build_distribution.sh` 的搬运):

```bash
cd /Users/admin/Workspaces/uniffle-master
mkdir -p jars/coordinator jars/server jars/dashboard

cp coordinator/target/coordinator-0.11.0-SNAPSHOT.jar jars/coordinator/
cp coordinator/target/jars/*                          jars/coordinator/

cp server/target/shuffle-server-0.11.0-SNAPSHOT.jar  jars/server/
cp server/target/jars/*                               jars/server/

cp dashboard/target/dashboard-0.11.0-SNAPSHOT.jar     jars/dashboard/
cp dashboard/target/jars/*                            jars/dashboard/
```

## 4. 启动三个服务

```bash
cd /Users/admin/Workspaces/uniffle-master

# 1) Coordinator(先起,Server 要向它注册)
bin/start-coordinator.sh

# 2) Shuffle Server
bin/start-shuffle-server.sh

# 3) Dashboard
bin/start-dashboard.sh
```

> 脚本靠 `utils.sh` 的 `load_rss_env` 自动定位 `RSS_HOME`/`conf`/`rss-env.sh`,无需额外环境变量。

### 验证进程与端口

```bash
jps -l | grep -E "CoordinatorServer|ShuffleServer|Dashboard"
# 应看到三行 org.apache.uniffle.* 进程

# HTTP 探活
curl -s -o /dev/null -w "coordinator: %{http_code}\n" http://localhost:19998/
curl -s -o /dev/null -w "dashboard:   %{http_code}\n" http://localhost:19997/
```

## 5. 打开 Dashboard

浏览器访问:

```
http://localhost:19997
```

页面结构(Vue 单页应用,左侧导航):
- **Dashboard** — 总览页,coordinator/server 概览
- **Coordinator Server** — coordinator 列表与状态
- **Shuffle Server** — shuffle server 列表(本机会出现 1 个,刚注册上来)
- **Application** — 应用列表(无作业时空)
- **Node List / Exclude Node List** — 节点与排除节点

dashboard 通过 `conf/dashboard.conf` 的 `coordinator.web.address http://localhost:19998` 经 Jetty 代理访问 coordinator 的 REST API 取数据,所以**必须先起 coordinator,dashboard 才有数据**。

## 6. 停止

```bash
bin/stop-dashboard.sh
bin/stop-shuffle-server.sh
bin/stop-coordinator.sh
```

## 7. 排错速查

| 现象 | 原因 / 处理 |
|---|---|
| build 报 `apache-incubator-disclaimer-resource-bundle ... absent` | 见第 1 步,手动装该包 |
| build 卡在下载 node/npm | 公司镜像慢,耐心等;或挂代理 |
| `No env JAVA_HOME` | 第 2.2 步 rss-env.sh 没设 JAVA_HOME |
| ShuffleServer 启动 OOM | server.conf 的 buffer.capacity / rss-env.sh 的 SHUFFLE_SERVER_XMX_SIZE 没调小 |
| dashboard 打开空白 | jar 内已含前端(`static/index.html`);检查 coordinator 是否已起、19998 是否通 |
| 端口被占用 | `lsof -i:19999` 等,杀掉残留进程 |
| server.conf `MEMORY_LOCALFILE_HDFS` 报错 | 本地无 HDFS,改 `MEMORY_LOCALFILE` |

## 附:日志与 PID 位置

- 日志:`$RSS_HOME/logs/{coordinator,shuffle_server,dashboard}.log`(及 `.out`)
- PID:`$RSS_HOME/{coordinator,shuffle-server,dashboard}.pid`

## 附:Dashboard 架构与 API 数据来源

### 为什么 dashboard 要单独部署

Dashboard 是一个**无状态的 HTTP 反向代理 + 静态资源服务器**(Jetty 进程),不是"调度系统"。它单独成进程有三个原因:

1. **聚合 HA 的 Coordinator 集群**。生产里 coordinator 是多节点 HA(见 `dashboard.conf` 的 `coordinator.web.address` 为逗号分隔的多个地址)。Dashboard 启动时把多个地址解析成 map,浏览器只需记一个 dashboard 地址即可在多个 coordinator 间切换。
2. **代理 Shuffle Server 的 HTTP 接口**。Shuffle Server 自己也有 Jetty HTTP 接口(暴露 metrics/conf/stacks)。生产有几十~几百台 server,浏览器无法逐台直连(内网/鉴权/数量),由 dashboard 这一层统一代理转发。
3. **托管前端静态资源**。Vue 构建产物打包在 `dashboard-*.jar` 的 `static/` 下,由 Jetty `ResourceHandler` 托管,`index.html` 为 welcome 文件。

```
浏览器 ──► Dashboard (19997)
              │  静态资源 /web/*         (自己提供)
              │  反向代理 /api/*  ──┬──► Coordinator (19998) REST API
              │                     └──► ShuffleServer (19978) REST API
```

它无状态、不存数据、不依赖存储层,停了重启不影响 shuffle 业务。

### 数据从哪个 API 来

前端请求分两路(`http.js` 的 `fontBackFlag` 参数控制):

| flag | axios 实例 | baseURL | 落点 |
|---|---|---|---|
| `1` | frontEnd | `/web` | Dashboard 自己的 servlet |
| `0` | backEnd | `/api` | Dashboard 反向代理 → coordinator/server |

`/api/*` 落到 `WebProxyServlet.rewriteTarget()`,按两个自定义请求头路由:

- `requestServerType: coordinator` → 用配置 map 取出 coordinator 地址,转发 `{coordinator}/api/{path}`
- `requestServerType: server` → 用 header 里的 `targetAddress`(某台 shuffle server 的 web 地址)转发

**数据来源映射:**

打给 **Coordinator**(经代理):
- `/server/nodes/summary` — 节点总数
- `/server/nodes?status=active|lost|unhealthy|decommissioning|decommissioned|excluded` — 各状态节点列表
- `/server/addExcludeNodes` `/server/removeExcludeNodes` — 加/删黑名单(POST)
- `/app/total` `/app/appInfos` `/app/userTotal` — 应用总数/列表/按用户
- `/server/deleteServer` — 删除确认丢失的 server(DELETE)
- `/coordinator/conf` `/coordinator/metrics` `/coordinator/stacks` — coordinator 自身配置/指标/线程栈

打给 **Shuffle Server**(经代理,`targetAddress` 指向具体某台):
- `/shuffleServer/conf` — server 配置
- `/shuffleServer/metrics` `/shuffleServer/prometheus/metrics/all` — 指标
- `/shuffleServer/stacks` — jvm 线程栈

打给 **Dashboard 自己**(`/web`,不代理):
- `/dashboard/info` `/dashboard/conf` — dashboard 自身信息/配置
- `/coordinator/coordinatorServers` — 可选的 coordinator 地址列表(从 `dashboard.conf` 读出)

> 一句话:页面骨架和 dashboard 自身配置走自己的 `/web/*`;所有真正的集群数据(shuffle server 列表、应用、metrics、黑名单)都是经 `/api/*` 代理到 coordinator 和 shuffle server 的 HTTP 接口拿的。**所以 coordinator 必须先起**,否则 dashboard 代理过去拿不到数据,页面会 403 或空。
