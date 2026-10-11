# Apache Uniffle 架构深度分析

## 目录
1. [项目概述](#1-项目概述)
2. [整体架构](#2-整体架构)
3. [核心模块详解](#3-核心模块详解)
4. [数据流分析](#4-数据流分析)
5. [关键技术决策](#5-关键技术决策)
6. [架构挑战与改进方向](#6-架构挑战与改进方向)

---

## 1. 项目概述

Apache Uniffle 是一个高性能、通用的远程 Shuffle 服务，为分布式计算引擎（Spark、MapReduce、Tez）提供统一的中间数据管理能力。它将传统的 "本地文件拉取式" Shuffle 转变为 "远程块推送式" Shuffle，实现了计算与存储的分离。

### 1.1 核心特性

- **计算存储分离**：支持计算节点与存储节点的独立扩展，提升资源利用率
- **多引擎支持**：Apache Spark (2.3/2.4/3.x)、Apache Hadoop MapReduce、Apache Tez
- **多级存储**：内存、本地磁盘、HDFS、远程存储的组合策略
- **高弹性**：支持动态扩缩容，适应云原生环境
- **高可用**：Coordinator 支持多节点部署，Shuffle Server 支持多副本
- **动态配置**：支持客户端动态配置，无需重启服务

### 1.2 技术栈

| 组件 | 技术 | 版本 |
|------|------|------|
| 构建工具 | Apache Maven | 3.6+ |
| 语言 | Java | 1.8+ (推荐 11+) |
| RPC 框架 | gRPC + Netty | 1.44+ / 4.1+ |
| 序列化 | Protocol Buffers | 3.x |
| 存储后端 | Local File / HDFS / 内存 | - |
| 监控指标 | Prometheus | - |

---

## 2. 整体架构

### 2.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Apache Uniffle Cluster                           │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                      Coordinator 集群                               │  │
│  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐        │  │
│  │  │ Coordinator  │◀───▶│ Coordinator  │◀───▶│ Coordinator  │        │  │
│  │  │     (主)     │     │   (备1)      │     │   (备2)      │        │  │
│  │  └──────┬───────┘     └──────────────┘     └──────────────┘        │  │
│  │         │                                                          │  │
│  │         │  元数据管理：Shuffle Server 管理、任务分配、应用生命周期      │  │
│  │         ▼                                                          │  │
│  │  ┌─────────────────────────────────────────────────────────────┐   │  │
│  │  │              ClusterManager / ApplicationManager             │   │  │
│  │  │              - Shuffle Server 注册与心跳管理                  │   │  │
│  │  │              - 应用 Shuffle 分配策略                          │   │  │
│  │  │              - 访问控制与配额管理                             │   │  │
│  │  └─────────────────────────────────────────────────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                       │
│         ┌──────────────────────────┼──────────────────────────┐            │
│         │                          │                          │            │
│         ▼                          ▼                          ▼            │
│  ┌──────────────┐          ┌──────────────┐          ┌──────────────┐     │
│  │Shuffle Server│          │Shuffle Server│          │Shuffle Server│     │
│  │      1       │◀────────▶│      2       │◀────────▶│      3       │     │
│  └──────┬───────┘          └──────┬───────┘          └──────┬───────┘     │
│         │                         │                         │              │
│  ┌──────┴───────┐          ┌──────┴───────┐          ┌──────┴───────┐     │
│  │ - BufferMgr  │          │ - BufferMgr  │          │ - BufferMgr  │     │
│  │ - FlushMgr   │          │ - FlushMgr   │          │ - FlushMgr   │     │
│  │ - TaskMgr    │          │ - TaskMgr    │          │ - TaskMgr    │     │
│  │ - StorageMgr │          │ - StorageMgr │          │ - StorageMgr │     │
│  └──────────────┘          └──────────────┘          └──────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ▲
                                       │ gRPC / Netty RPC
┌──────────────────────────────────────┼─────────────────────────────────────┐
│                     Compute Engine     │                                     │
│  ┌─────────────────────────────────────┴─────────────────────────────────┐  │
│  │                           Client 层                                    │  │
│  │  ┌─────────────────────┐         ┌─────────────────────────────────┐  │  │
│  │  │  ShuffleManager     │         │        ShuffleWriter            │  │  │
│  │  │  (Driver/AM 端)      │         │    (Executor/Task 端)           │  │  │
│  │  │                     │         │                                 │  │  │
│  │  │ - 申请 Shuffle 分配  │◀───────▶│ - 推送 Shuffle 数据              │  │  │
│  │  │ - 协调数据读取       │         │ - 失败重试处理                   │  │  │
│  │  │ - 任务完成确认       │         │ - 数据压缩/解压                  │  │  │
│  │  └─────────────────────┘         └─────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 模块依赖关系

```
                    ┌─────────────────┐
                    │   Coordinator   │
                    └────────┬────────┘
                             │ RPC
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│Shuffle Server │◀──▶│    Common     │◀──▶│    Client     │
└───────┬───────┘    └───────┬───────┘    └───────┬───────┘
        │                    │                    │
        │            ┌───────┴───────┐            │
        │            │               │            │
        └───────────▶│    Proto      │◀───────────┘
                     │   (gRPC/PB)   │
                     └───────────────┘
```

---

## 3. 核心模块详解

### 3.1 Common 模块 (基础层)

**职责**：提供所有模块共享的基础设施

```
common/src/main/java/org/apache/uniffle/common/
├── config/                     # 配置系统
│   ├── RssBaseConf.java       # 基础配置类
│   ├── RssClientConf.java     # 客户端配置
│   └── ConfigUtils.java       # 配置工具
├── metrics/                    # 指标系统
│   ├── GRPCMetrics.java
│   ├── JvmMetrics.java
│   └── MetricReporter.java
├── rpc/                        # RPC 基础设施
│   ├── ServerInterface.java   # 服务端接口
│   └── ServerType.java        # 服务类型枚举
├── security/                   # 安全认证
│   ├── SecurityConfig.java    # 安全配置
│   └── SecurityContextFactory.java
├── web/                        # Web 服务
│   └── JettyServer.java       # Jetty HTTP 服务
├── ShuffleBlockInfo.java       # Shuffle 块信息
├── ShuffleServerInfo.java      # Shuffle Server 信息
├── StorageType.java            # 存储类型枚举
└── util/                       # 工具类
    ├── RssUtils.java
    └── Constants.java
```

**关键技术点**：
- **配置系统**：基于类型安全的配置项定义，支持动态重新配置
- **指标系统**：集成 Prometheus，支持多维度指标采集
- **安全认证**：支持 Hadoop Kerberos 认证

### 3.2 Coordinator 模块 (控制层)

**职责**：集群元数据管理、Shuffle Server 分配、应用生命周期管理

```
coordinator/src/main/java/org/apache/uniffle/coordinator/
├── CoordinatorServer.java          # 主服务入口
├── CoordinatorConf.java            # Coordinator 配置
├── CoordinatorGrpcService.java     # gRPC 服务实现
├── ClusterManager.java             # 集群管理接口
├── SimpleClusterManager.java       # 集群管理实现
├── ApplicationManager.java         # 应用管理器
├── AssignmentStrategy.java         # 分配策略接口
├── AssignmentStrategyFactory.java  # 分配策略工厂
├── AccessManager.java              # 访问控制管理
├── QuotaManager.java               # 配额管理
├── ServerNode.java                 # Shuffle Server 节点
└── web/resource/                   # REST API
```

**核心功能**：

1. **Shuffle Server 管理**
   - 注册/心跳检测/离线处理
   - 磁盘状态监控
   - 负载均衡调度

2. **分配策略**
   ```java
   // 支持多种分配策略
   public interface AssignmentStrategy {
     ShuffleAssignmentsInfo assignShuffleServers(...);
   }
   
   // 实现类：
   // - PartitionBalanceAssignmentStrategy (分区均衡)
   // - AppBalanceAssignmentStrategy (应用均衡)
   ```

3. **动态配置管理**
   - 客户端配置集中管理
   - 支持配置文件热更新
   - 多租户配置隔离

### 3.3 Shuffle Server 模块 (数据层)

**职责**：接收 Shuffle 数据、缓冲管理、数据刷盘、数据读取

```
server/src/main/java/org/apache/uniffle/server/
├── ShuffleServer.java              # 主服务入口 (600+ 行)
├── ShuffleServerConf.java          # 服务配置
├── ShuffleServerGrpcService.java   # gRPC 服务实现
├── ShuffleTaskManager.java         # Shuffle 任务管理
├── ShuffleFlushManager.java        # 数据刷盘管理
├── ShuffleBufferManager.java       # 缓冲区管理
├── RegisterHeartBeat.java          # 心跳注册
├── HealthCheck.java                # 健康检查
├── StorageManager.java             # 存储管理接口
├── buffer/                         # 缓冲区实现
│   ├── ShuffleBuffer.java
│   ├── ShuffleBufferManager.java
│   └── ShuffleBufferType.java
├── storage/                        # 存储实现
│   ├── StorageManagerFactory.java
│   ├── LocalStorageManager.java
│   └── HadoopStorageManager.java
└── merge/                          # 远程排序合并
    └── ShuffleMergeManager.java
```

**存储架构**：
```
┌─────────────────────────────────────────────────────────────┐
│                    ShuffleBufferManager                     │
│  管理内存缓冲区，接收客户端推送的数据                           │
├─────────────────────────────────────────────────────────────┤
│                    ShuffleFlushManager                      │
│  管理刷盘线程池，将内存数据异步写入存储                         │
├─────────────────────────────────────────────────────────────┤
│                     StorageManager                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │
│  │   Memory    │ │ Local Disk  │ │       HDFS          │   │
│  │             │ │             │ │                     │   │
│  │ 内存缓冲区   │ │ 本地索引文件 │ │ 远程存储备份        │   │
│  │ 热数据读取   │ │ 数据文件    │ │ 冷数据归档          │   │
│  └─────────────┘ └─────────────┘ └─────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**数据文件格式**：
```
Index File (索引文件)          Data File (数据文件)
┌─────────────────┐           ┌─────────────────┐
│ Block 1 Offset  │──────────▶│   Block 1 Data  │
│ Block 1 Length  │           ├─────────────────┤
│ Block 1 CRC     │           │   Block 2 Data  │
├─────────────────┤           ├─────────────────┤
│ Block 2 Offset  │──────────▶│   Block 3 Data  │
│ Block 2 Length  │           ├─────────────────┤
│ Block 2 CRC     │           │      ...        │
├─────────────────┤           └─────────────────┘
│      ...        │
└─────────────────┘
```

### 3.4 Client 模块 (接入层)

**职责**：为计算引擎提供 Shuffle 接口适配

```
client/                         # 公共客户端接口
├── src/main/java/
│   └── org/apache/uniffle/client/
│       └── PartitionDataReplicaRequirementTracking.java

client-spark/                   # Spark 适配
├── common/                     # Spark 通用代码
├── spark2/                     # Spark 2.x 适配
└── spark3/                     # Spark 3.x 适配

client-mr/                      # MapReduce 适配
├── core/
├── hadoop2.8/
└── hadoop3.2/

client-tez/                     # Tez 适配
└── src/
```

**Spark Shuffle 集成**：
```java
// Spark 2.x
spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager

// Spark 3.x + 动态分配
spark.shuffle.sort.io.plugin.class org.apache.spark.shuffle.RssShuffleDataIo
```

### 3.5 Storage 模块 (存储层)

**职责**：提供可插拔的存储后端实现

```
storage/src/main/java/org/apache/uniffle/storage/
├── StorageFactory.java           # 存储工厂
├── handler/                      # 存储处理器
│   ├── StorageReadHandler.java
│   └── StorageWriteHandler.java
├── impl/                         # 存储实现
│   ├── HadoopShuffleWriteHandler.java
│   ├── HadoopShuffleReadHandler.java
│   ├── LocalFileShuffleWriteHandler.java
│   └── LocalFileShuffleReadHandler.java
├── request/                      # 存储请求
├── response/                     # 存储响应
└── util/                         # 存储工具
```

**存储类型**：

| 存储类型 | 描述 | 适用场景 |
|---------|------|---------|
| MEMORY | 仅内存存储 | 测试环境 |
| MEMORY_LOCALFILE | 内存 + 本地磁盘 | 中小规模集群 |
| MEMORY_LOCALFILE_HDFS | 内存 + 本地磁盘 + HDFS | 生产环境（推荐） |
| LOCALFILE | 仅本地磁盘 | 测试环境 |
| HDFS | 仅 HDFS | 测试环境 |

### 3.6 Proto 模块 (协议层)

**职责**：定义 gRPC 服务和消息协议

```
proto/src/main/proto/Rss.proto
```

**核心服务定义**：
```protobuf
service ShuffleServer {
  // Shuffle 注册与注销
  rpc registerShuffle(ShuffleRegisterRequest) returns (ShuffleRegisterResponse);
  rpc unregisterShuffle(ShuffleUnregisterRequest) returns (ShuffleUnregisterResponse);
  
  // 数据写入
  rpc sendShuffleData(SendShuffleDataRequest) returns (SendShuffleDataResponse);
  rpc requireBuffer(RequireBufferRequest) returns (RequireBufferResponse);
  
  // 数据读取
  rpc getLocalShuffleData(GetLocalShuffleDataRequest) returns (GetLocalShuffleDataResponse);
  rpc getLocalShuffleIndex(GetLocalShuffleIndexRequest) returns (GetLocalShuffleIndexResponse);
  rpc getMemoryShuffleData(GetMemoryShuffleDataRequest) returns (GetMemoryShuffleDataResponse);
  
  // 任务管理
  rpc commitShuffleTask(ShuffleCommitRequest) returns (ShuffleCommitResponse);
  rpc finishShuffle(FinishShuffleRequest) returns (FinishShuffleResponse);
  rpc appHeartbeat(AppHeartBeatRequest) returns (AppHeartBeatResponse);
  
  // 远程排序合并
  rpc startSortMerge(StartSortMergeRequest) returns (StartSortMergeResponse);
  rpc getSortedShuffleData(GetSortedShuffleDataRequest) returns (GetSortedShuffleDataResponse);
}
```

---

## 4. 数据流分析

### 4.1 Shuffle Write 流程

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│ Executor │───▶│ShuffleWriter │───▶│Shuffle Server│───▶│ Storage  │
└──────────┘    └──────────────┘    └──────────────┘    └──────────┘
     │                 │                   │
     │ 1. sendShuffleData()                │
     │────────────────▶│                   │
     │                 │ 2. 写入内存缓冲区   │
     │                 │──────────────────▶│
     │                 │                   │
     │                 │ 3. 缓冲区满时触发刷盘 │
     │                 │──────────────────▶│──────▶ Local File / HDFS
     │                 │                   │
     │ 4. 返回确认     │                   │
     │◀────────────────│                   │
     │                 │                   │
     │ 5. 任务完成报告 blockId             │
     │────────────────────────────────────▶│
```

**详细步骤**：
1. Executor 通过 `ShuffleWriter` 发送数据到 `ShuffleBufferManager`
2. 数据先缓存到内存缓冲区
3. 缓冲区满或达到阈值时，`ShuffleFlushManager` 异步刷盘
4. 写入索引文件和数据文件
5. 任务完成后向 Shuffle Server 报告所有 blockId 用于数据校验

### 4.2 Shuffle Read 流程

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│ Executor │───▶│ShuffleReader │───▶│Shuffle Server│───▶│ Storage  │
└──────────┘    └──────────────┘    └──────────────┘    └──────────┘
     │                 │                   │
     │ 1. 从 Driver 获取 Shuffle Server 分配信息
     │◀────────────────┘                   │
     │                                     │
     │ 2. 请求索引 (getLocalShuffleIndex)   │
     │────────────────────────────────────▶│──────▶ 读取索引文件
     │                                     │
     │ 3. 返回索引信息                      │
     │◀────────────────────────────────────│
     │                                     │
     │ 4. 请求数据 (getLocalShuffleData)    │
     │────────────────────────────────────▶│──────▶ 读取数据文件
     │                                     │
     │ 5. 返回数据块                        │
     │◀────────────────────────────────────│
```

### 4.3 容错机制

```
┌────────────────────────────────────────────────────────────────┐
│                        容错策略                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌─────────────────┐      ┌─────────────────┐                 │
│  │   写入失败      │      │   读取失败      │                 │
│  └────────┬────────┘      └────────┬────────┘                 │
│           │                        │                          │
│           ▼                        ▼                          │
│  ┌─────────────────┐      ┌─────────────────┐                 │
│  │ 1. 重试其他副本  │      │ 1. 重试其他副本  │                 │
│  │ 2. 标记失败节点  │      │ 2. 标记失败节点  │                 │
│  │ 3. 报告 Coordinator│    │ 3. 报告 Coordinator            │
│  └─────────────────┘      └─────────────────┘                 │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    Shuffle Server 离线                   │  │
│  │  - Coordinator 检测心跳超时                              │  │
│  │  - 标记节点为不可用                                      │  │
│  │  - 新任务不再分配到该节点                                │  │
│  │  - 支持退役 (Decommission) 机制                          │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    推测执行支持                          │  │
│  │  - 存储 taskAttemptId 到 MapStatus                       │  │
│  │  - 支持 Spark Speculation                                │  │
│  │  - 自动去重同一任务的多次尝试                             │  │
│  └─────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## 5. 关键技术决策

### 5.1 为什么选择 gRPC + Netty？

**设计考虑**：
- **性能**：gRPC 基于 HTTP/2，支持多路复用和流控制
- **兼容性**：Protobuf 提供高效、跨语言的序列化
- **生态**：成熟的 RPC 框架，支持服务发现和负载均衡
- **双协议支持**：gRPC 用于控制流，Netty 可用于大数据流传输

**协议分层**：
```
┌─────────────────────────────────────────┐
│            Application Layer            │
│     (SendShuffleData / GetShuffleData) │
├─────────────────────────────────────────┤
│           Protocol Buffers              │
│         (Message Serialization)        │
├─────────────────────────────────────────┤
│              gRPC / Netty               │
│      (RPC Framework / Transport)       │
├─────────────────────────────────────────┤
│              HTTP/2 / TCP              │
└─────────────────────────────────────────┘
```

### 5.2 多级存储架构设计

**存储策略选择**：

| 存储类型 | 优点 | 缺点 | 推荐场景 |
|---------|------|------|---------|
| MEMORY | 最低延迟 | 数据易失，容量受限 | 测试 |
| MEMORY_LOCALFILE | 低延迟，数据持久化 | 单机容量受限 | 小规模 |
| MEMORY_LOCALFILE_HDFS | 容量无限，高可靠 | HDFS 延迟较高 | 生产环境 |

**数据冷热分离**：
```
┌─────────────────────────────────────────────────────────┐
│                     数据生命周期                         │
├─────────────────────────────────────────────────────────┤
│  热数据 (Hot)   →   温数据 (Warm)   →   冷数据 (Cold)   │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐       │
│  │  Memory │──────▶│  Local  │──────▶│  HDFS   │       │
│  │  Buffer │       │  Disk   │       │         │       │
│  └─────────┘       └─────────┘       └─────────┘       │
│      写入              刷盘              归档            │
│   (< 几分钟)        (几小时)          (任务结束)        │
└─────────────────────────────────────────────────────────┘
```

### 5.3 内存管理策略

```java
// ShuffleBufferManager 的内存分级管理
1. 预分配内存 (PreAllocated)  - 为待写入数据预留
2. 已用内存 (Used)            - 缓冲区中实际数据
3. 读取缓冲 (Read Buffer)      - 数据读取缓冲
4. 直接内存 (Direct Memory)    - Netty 使用

// 内存压力处理
- 内存不足时：暂停接收新数据 (back pressure)
- 刷盘线程：异步将内存数据写入磁盘
- 清理机制：任务完成后释放相关内存
```

**JDK 版本建议**：

| JDK 版本 | GC 算法 | 最大暂停时间 | 吞吐量 | 推荐度 |
|---------|---------|-------------|--------|--------|
| 8 | G1 | ~30s | 较低 | ⭐⭐ |
| 11 | G1 | ~2.5s | 高 | ⭐⭐⭐⭐ |
| 17+ | ZGC | ~0.2ms | 极高 | ⭐⭐⭐⭐⭐ |

### 5.4 配置管理设计

**三层配置体系**：
```
┌─────────────────────────────────────────────┐
│           Configuration Layers              │
├─────────────────────────────────────────────┤
│ 1. 客户端自定义配置    (最高优先级)          │
├─────────────────────────────────────────────┤
│ 2. Coordinator 动态配置 (动态更新)           │
├─────────────────────────────────────────────┤
│ 3. 配置文件默认配置    (基础配置)            │
└─────────────────────────────────────────────┘
```

---

## 6. 架构挑战与改进方向

### 6.1 当前架构挑战

#### 6.1.1 代码复杂度

| 文件 | 行数 | 问题 |
|-----|------|------|
| ShuffleServer.java | ~650 | 职责较多，可进一步拆分 |
| ShuffleTaskManager.java | ~800 | 任务管理逻辑复杂 |
| RssBaseConf.java | ~1000+ | 配置项众多 |

#### 6.1.2 高可用性

- Coordinator 目前依赖外部协调（如手动配置多节点）
- 缺少内置的 Leader 选举机制
- Shuffle Server 数据副本策略需手动配置

#### 6.1.3 云原生支持

- Kubernetes Operator 已提供，但功能可进一步增强
- 缺少自动扩缩容机制
- 服务网格集成有待完善

### 6.2 改进方向

#### 6.2.1 短期改进 (3-6 个月)

1. **模块化重构**
   - 拆分 ShuffleServer 的大类
   - 提取存储层抽象接口
   - 优化配置系统结构

2. **可观测性增强**
   - 完善 Metrics 指标覆盖
   - 增加分布式追踪支持
   - 优化日志结构化输出

3. **性能优化**
   - 内存池化管理
   - 零拷贝数据传输
   - 批量操作优化

#### 6.2.2 中期改进 (6-12 个月)

1. **存储层增强**
   - 支持更多云存储（S3、OSS、GCS）
   - 实现分层存储自动迁移
   - 数据压缩算法优化

2. **高可用增强**
   - Coordinator 内置 HA 机制
   - 自动故障检测与恢复
   - 数据副本自动均衡

3. **多租户增强**
   - 资源隔离机制
   - 优先级调度
   - 配额实时监控

#### 6.2.3 长期演进 (12 个月+)

1. **云原生架构**
   - 无状态化设计
   - 自动弹性扩缩容
   - 多集群联邦

2. **新协议支持**
   - HTTP/3 数据传输
   - RDMA 支持
   - 内核旁路技术

3. **智能优化**
   - 基于 ML 的缓存策略
   - 自适应压缩算法
   - 预测性资源调度

---

## 7. 附录

### 7.1 关键配置文件

```
conf/
├── coordinator.conf          # Coordinator 配置
├── server.conf               # Shuffle Server 配置
├── rss-env.sh                # 环境变量
├── dynamic_client.conf       # 动态客户端配置
├── exclude_nodes             # 排除节点列表
└── log4j2.xml                # 日志配置
```

### 7.2 重要工具脚本

```
bin/
├── start-coordinator.sh      # 启动 Coordinator
├── start-shuffle-server.sh   # 启动 Shuffle Server
├── stop.sh                   # 停止服务
└── rss-cli.sh                # CLI 工具

sbin/
└── (系统级管理脚本)

dev/
└── (开发工具)
```

### 7.3 参考资源

- **官方文档**: https://uniffle.apache.org/
- **GitHub**: https://github.com/apache/incubator-uniffle
- **Jira**: https://issues.apache.org/jira/projects/UNIFFLE
- **邮件列表**: dev@uniffle.apache.org

---

## 8. 总结

Apache Uniffle 通过三层架构（Coordinator - Shuffle Server - Client）实现了计算与存储的分离，有效解决了大数据计算中的 Shuffle 瓶颈问题。其设计理念包括：

1. **解耦**：计算引擎与存储层解耦，独立扩展
2. **灵活**：支持多种存储后端和计算引擎
3. **可靠**：多级容错机制保障数据安全
4. **高性能**：内存缓冲 + 异步刷盘优化写入性能

随着云原生技术的发展，Uniffle 在弹性伸缩、高可用、多租户等方面仍有较大的提升空间，值得持续关注和贡献。
