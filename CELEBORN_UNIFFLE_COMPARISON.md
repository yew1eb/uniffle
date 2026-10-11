# Apache Celeborn vs Apache Uniffle 深度对比分析

> 分析日期：2026-03-21
> 对比版本：Celeborn main / Uniffle main

---

## 目录

1. [项目背景对比](#1-项目背景对比)
2. [架构设计对比](#2-架构设计对比)
3. [功能特性对比](#3-功能特性对比)
4. [性能对比](#4-性能对比)
5. [代码实现对比](#5-代码实现对比)
6. [部署与运维对比](#6-部署与运维对比)
7. [优缺点总结](#7-优缺点总结)
8. [选型建议](#8-选型建议)

---

## 1. 项目背景对比

### 1.1 基本信息

| 属性 | Apache Celeborn | Apache Uniffle |
|------|-----------------|----------------|
| **项目原名** | Remote Shuffle Service (RSS) | Remote Shuffle Service (RSS) |
| **捐赠 Apache** | 2022 年 | 2022 年 |
| **当前状态** | Apache 孵化项目 | Apache 孵化项目 |
| **主要贡献者** | Alibaba / Apache Flink 社区 | Tencent / Apache Spark 社区 |
| **GitHub** | https://github.com/apache/celeborn | https://github.com/apache/incubator-uniffle |
| **起源** | 阿里云 EMR 团队内部项目 | 腾讯云大数据团队内部项目 |

### 1.2 版本演进

**Celeborn**:
- 0.1.0 (2022): 初始 Apache 版本
- 0.2.x (2023): 引入 TierWriter，支持 S3/OSS
- 0.3.x (2023): Master HA 完善，Flink 支持增强
- 0.4.x (2024): 性能优化，监控增强
- 0.5.x (2025): 云原生支持，多租户增强

**Uniffle**:
- 0.1.0 (2022): 初始 Apache 版本
- 0.2.0 (2022): 基础功能完善
- 0.7.x (2023): Netty 支持，性能优化
- 0.8.x (2024): 远程合并，Kubernetes Operator
- 0.9.x (2024): Netty 模式稳定，大规模生产验证

### 1.3 社区活跃度

| 指标 | Celeborn | Uniffle |
|-----|----------|---------|
| GitHub Stars | ~2,000 | ~1,500 |
| 贡献者数量 | ~100 | ~80 |
| 月度 Commit | ~50 | ~30 |
| 生产用户 | 阿里云、字节跳动等 | 腾讯云、B站等 |

---

## 2. 架构设计对比

### 2.1 系统架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Celeborn 架构                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Master 集群 (HA via Ratis)                     │   │
│  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐        │   │
│  │  │    Master    │◀───▶│    Master    │◀───▶│    Master    │        │   │
│  │  │   (Leader)   │ Raft│  (Follower)  │ Raft│  (Follower)  │        │   │
│  │  └──────────────┘     └──────────────┘     └──────────────┘        │   │
│  │                                                                    │   │
│  │  功能: 元数据管理 (Worker 状态、Slot 分配、应用生命周期)              │   │
│  │       基于 Ratis 实现强一致性 HA                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│         ┌──────────────────────────┼──────────────────────────┐             │
│         │                          │                          │             │
│         ▼                          ▼                          ▼             │
│  ┌──────────────┐          ┌──────────────┐          ┌──────────────┐      │
│  │    Worker    │          │    Worker    │          │    Worker    │      │
│  │      1       │◀────────▶│      2       │◀────────▶│      3       │      │
│  └──────────────┘          └──────────────┘          └──────────────┘      │
│                                                                             │
│  组件: PushServer / FetchServer / StorageManager / MemoryManager           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                            Uniffle 架构                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Coordinator 集群                               │   │
│  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐        │   │
│  │  │ Coordinator  │◀───▶│ Coordinator  │◀───▶│ Coordinator  │        │   │
│  │  │   (无状态)    │     │   (无状态)    │     │   (无状态)    │        │   │
│  │  └──────────────┘     └──────────────┘     └──────────────┘        │   │
│  │                                                                    │   │
│  │  功能: 元数据管理 (Shuffle Server 注册、任务分配、动态配置)           │   │
│  │       无状态设计，依赖外部存储（可选）                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│         ┌──────────────────────────┼──────────────────────────┐             │
│         │                          │                          │             │
│         ▼                          ▼                          ▼             │
│  ┌──────────────┐          ┌──────────────┐          ┌──────────────┐      │
│  │Shuffle Server│          │Shuffle Server│          │Shuffle Server│      │
│  │      1       │◀────────▶│      2       │◀────────▶│      3       │      │
│  └──────────────┘          └──────────────┘          └──────────────┘      │
│                                                                             │
│  组件: BufferManager / FlushManager / TaskManager / StorageManager         │
│        支持 gRPC / Netty 双协议                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 架构差异对比

| 对比项 | Celeborn | Uniffle | 分析 |
|-------|----------|---------|------|
| **控制节点** | Master (有状态) | Coordinator (无状态) | Celeborn Master 维护全局状态，Uniffle Coordinator 无状态设计更简单 |
| **HA 机制** | Ratis Raft 共识 | 多节点 + 客户端重试 | Celeborn 内置强一致 HA，Uniffle 依赖外部机制 |
| **Worker 通信** | Push/Fetch 分离 | 统一 gRPC/Netty 服务 | Celeborn 职责分离更清晰，Uniffle 部署更简单 |
| **元数据存储** | Ratis 日志复制 | 内存 + 可选持久化 | Celeborn 强一致，Uniffle 更轻量 |
| **状态管理** | Master 集中管理 | 各节点自治 | Celeborn 集中管理便于全局优化，Uniffle 容错性更好 |

### 2.3 高可用对比

**Celeborn HA**:
```
┌─────────────────────────────────────────┐
│           Celeborn HA 架构              │
├─────────────────────────────────────────┤
│                                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
│  │ Master0 │  │ Master1 │  │ Master2 │ │
│  │ Leader  │  │Follower │  │Follower │ │
│  │  (Active)│  │ (Sync)  │  │ (Sync)  │ │
│  └────┬────┘  └─────────┘  └─────────┘ │
│       │                                 │
│       │ Ratis 日志复制 (强一致性)        │
│       ▼                                 │
│  ┌─────────┐                            │
│  │ Worker  │◀── 心跳/状态同步            │
│  └─────────┘                            │
│                                         │
│  故障切换: 自动选举，秒级恢复             │
│  数据一致性: 强一致                       │
│  性能开销: 10-100x 延迟放大               │
│                                         │
└─────────────────────────────────────────┘
```

**Uniffle HA**:
```
┌─────────────────────────────────────────┐
│           Uniffle HA 架构               │
├─────────────────────────────────────────┤
│                                         │
│  ┌─────────────┐  ┌─────────────┐      │
│  │Coordinator 1│  │Coordinator 2│      │
│  │  (Active)   │  │  (Standby)  │      │
│  └──────┬──────┘  └─────────────┘      │
│         │                               │
│         │ 无状态，可横向扩展             │
│         ▼                               │
│  ┌─────────────┐                        │
│  │ShuffleServer│◀── 心跳注册到多个       │
│  └─────────────┘    Coordinator         │
│                                         │
│  故障切换: 客户端自动重试其他节点         │
│  数据一致性: 最终一致                     │
│  性能开销: 无额外延迟                     │
│                                         │
└─────────────────────────────────────────┘
```

### 2.4 存储架构对比

| 特性 | Celeborn | Uniffle |
|-----|----------|---------|
| **存储分层** | Memory → Local → HDFS → S3/OSS | Memory → Local → HDFS |
| **写入路径** | TierWriter 统一处理 | ShuffleBufferManager → FlushManager |
| **刷盘策略** | 统一配置，自适应调整 | 可配置，支持多种触发条件 |
| **副本机制** | 2 副本（Primary + Replica） | 可配置副本数 |
| **冷存储** | 支持 S3/OSS 直接写入 | 需通过 HDFS 中转 |

---

## 3. 功能特性对比

### 3.1 支持的计算引擎

| 引擎 | Celeborn | Uniffle | 备注 |
|-----|----------|---------|------|
| **Apache Spark** | ✅ 2.4/3.x/4.x | ✅ 2.3/2.4/3.x | 两者都支持完善 |
| **Apache Flink** | ✅ 1.16-2.2 | ❌ | Celeborn 对 Flink 支持更好 |
| **Apache MapReduce** | ✅ | ✅ | 两者都支持 |
| **Apache Tez** | ❌ | ✅ | Uniffle 独有 |

### 3.2 高级功能对比

| 功能 | Celeborn | Uniffle | 说明 |
|-----|----------|---------|------|
| **动态配置** | ✅ 热更新 | ✅ 热更新 | 两者都支持 |
| **多租户** | ✅ 配额管理 | ✅ 配额管理 | Celeborn 更完善 |
| **数据压缩** | ✅ LZ4/Zstd | ✅ LZ4/Zstd/Snappy | Uniffle 支持更多算法 |
| **远程合并** | ⚠️ 部分支持 | ✅ 完整支持 | Uniffle 支持服务端排序合并 |
| **推测执行** | ✅ | ✅ | 两者都支持 |
| **数据副本** | ✅ 2 副本 | ✅ 可配置 | Celeborn 固定 2 副本 |
| **异构存储** | ✅ HDD/SSD 自适应 | ⚠️ 简单支持 | Celeborn 更智能 |
| **云存储** | ✅ S3/OSS/GCS | ⚠️ HDFS only | Celeborn 云原生更好 |
| **K8s Operator** | ✅ | ✅ | 两者都提供 |
| **Prometheus 监控** | ✅ | ✅ | 两者都支持 |

### 3.3 配置系统对比

**Celeborn**:
```scala
// Scala 配置系统，类型安全
val workerFlusherBufferSize = ConfigBuilder(...)
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("256k")
    
val slotsAssignPolicy = ConfigBuilder(...)
    .enumConf(classOf[SlotsAssignPolicy])
    .createWithDefault(SlotsAssignPolicy.ROUNDBIN)
```

**Uniffle**:
```java
// Java 配置系统，支持动态重配置
public static final ConfigOption<Long> SERVER_BUFFER_CAPACITY =
    ConfigOptions.key("rss.server.buffer.capacity")
        .bytesType()
        .noDefaultValue();
        
public static final ConfigOption<ShuffleBufferType> SERVER_SHUFFLE_BUFFER_TYPE =
    ConfigOptions.key("rss.server.shuffle.buffer.type")
        .enumType(ShuffleBufferType.class)
        .defaultValue(ShuffleBufferType.LINKEDLIST);
```

**对比**:
| 特性 | Celeborn | Uniffle |
|-----|----------|---------|
| 配置语言 | Scala | Java |
| 类型安全 | ✅ | ✅ |
| 动态重配置 | ⚠️ 部分支持 | ✅ 完整支持 |
| 配置层级 | 系统/租户/用户 | 系统/动态客户端 |
| 热加载 | ⚠️ 需重启 | ✅ 支持 |

---

## 4. 性能对比

### 4.1 官方 Benchmark 数据

**Celeborn Benchmark** (公开数据较少):
- 测试环境：阿里云内部环境
- 主要场景：Flink 大状态作业
- 关键指标：未公开详细数据

**Uniffle Benchmark** (详细公开数据):

| 测试场景 | 配置 | 吞吐 | 延迟 |
|---------|-----|------|------|
| TPC-DS 1TB | Spark 2.4, MEMORY_LOCALFILE | 比 Vanilla Spark 慢 9% | - |
| TeraSort 1TB | Spark 2.4, MEMORY_LOCALFILE | 比 Vanilla Spark 快 30% | - |
| 大规模 Shuffle 10TB | Spark 3.3, Netty(SSD) | 单节点 0.93 GB/s 写, 1.56 GB/s 读 | E2E 12min |
| 高并发 11200 任务 | Spark 3.3, Netty(SSD) | 稳定运行 | E2E 13min |

### 4.2 gRPC vs Netty 性能

**Uniffle 官方测试** (0.9.0):

| 协议 | 并发数 | 单节点写入 | 单节点读取 | 稳定性 |
|-----|-------|-----------|-----------|-------|
| Netty | 1400 | 0.93 GB/s | 1.56 GB/s | ✅ 稳定 |
| gRPC | 1400 | 0.75 GB/s | 1.25 GB/s | ✅ 稳定 |
| Netty | 11200 | 0.86 GB/s | 1.44 GB/s | ✅ 稳定 |
| gRPC | 11200 | 0.62 GB/s | 1.04 GB/s | ⚠️ OOM 风险 |

**结论**: Uniffle 的 Netty 实现比 gRPC 快约 20%，且在高并发下更稳定。

### 4.3 扩展性对比

| 指标 | Celeborn | Uniffle |
|-----|----------|---------|
| **最大 Worker 数** | 1500+ | 100+ (已验证) |
| **最大并发任务** | 10,000+ | 11,200 (已验证) |
| **单节点吞吐** | 未公开 | 0.93 GB/s (写) |
| **数据规模** | PB 级 | 10TB+ (已验证) |

### 4.4 资源消耗对比

| 资源类型 | Celeborn | Uniffle | 说明 |
|---------|----------|---------|------|
| **内存使用** | 中等 | 较高 | Uniffle 缓冲区设计更激进 |
| **CPU 使用** | 中等 | 较低 | Uniffle Netty 模式 CPU 效率更高 |
| **磁盘 I/O** | 优化良好 | 优化良好 | 两者都支持 HDD/SSD 优化 |
| **网络开销** | 较低 | 较低 | 两者都支持压缩 |

---

## 5. 代码实现对比

### 5.1 编程语言与风格

| 维度 | Celeborn | Uniffle |
|-----|----------|---------|
| **主要语言** | Scala (70%) + Java (30%) | Java (95%) + Scala (5%) |
| **代码行数** | ~15 万行 | ~12 万行 |
| **设计风格** | 函数式 + 面向对象 | 纯面向对象 |
| **异步模型** | Netty Future + Scala Future | CompletableFuture |
| **配置方式** | Scala DSL | Java ConfigOption |

### 5.2 核心模块对比

**网络层**:

| 组件 | Celeborn | Uniffle |
|-----|----------|---------|
| RPC 框架 | Netty + gRPC | gRPC + Netty |
| 协议设计 | 自定义 Message 协议 | Protobuf + gRPC |
| 序列化 | 自定义 + Protobuf | Protobuf |
| 流控 | Credit-based | Back-pressure |
| 连接管理 | 连接池 | 连接复用 |

**存储层**:

| 组件 | Celeborn | Uniffle |
|-----|----------|---------|
| 缓冲区 | TierWriter (统一) | ShuffleBufferManager (分层) |
| 刷盘机制 | Flusher 线程池 | FlushEventHandler |
| 文件格式 | 索引 + 数据文件 | 索引 + 数据文件 |
| 存储后端 | Local/HDFS/S3/OSS | Local/HDFS |

**内存管理**:

| 组件 | Celeborn | Uniffle |
|-----|----------|---------|
| 内存管理器 | MemoryManager | ShuffleBufferManager |
| 直接内存 | 部分跟踪 | 部分跟踪 |
| 背压机制 | ServingState | 内存水位 |
| 缓冲复用 | SendBufferPool | 预分配机制 |

### 5.3 关键代码对比

**Worker/Shuffle Server 启动**:

**Celeborn (Scala)**:
```scala
class Worker(...)
  extends IsolatedRpcEndpoint
  with PushDataHandler
  with FetchDataHandler {
  
  override def onStart(): Unit = {
    storageManager.initialize()
    memoryManager.start()
    fetchServer.start()
    pushServer.start()
  }
}
```

**Uniffle (Java)**:
```java
public class ShuffleServer {
    public void start() throws Exception {
        jettyPort = jettyServer.start();
        grpcPort = server.start();
        if (nettyServerEnabled) {
            nettyPort = streamServer.start();
        }
        registerHeartBeat.startHeartBeat();
    }
}
```

**内存管理**:

**Celeborn**:
```java
public class MemoryManager {
    private final AtomicLong memoryUsage = new AtomicLong(0);
    private volatile ServingState servingState = ServingState.SERVING;
    
    public boolean isWritable() {
        return servingState == ServingState.SERVING;
    }
    
    public void updateMemoryUsage(long delta) {
        long newUsage = memoryUsage.addAndGet(delta);
        checkServingState(newUsage);
    }
}
```

**Uniffle**:
```java
public class ShuffleBufferManager {
    private final AtomicLong usedMemory = new AtomicLong(0);
    private final long highWaterMark;
    private final long lowWaterMark;
    
    public boolean requireBuffer(int size) {
        if (usedMemory.get() + size > highWaterMark) {
            return false;  // 触发背压
        }
        return usedMemory.addAndGet(size) <= capacity;
    }
}
```

### 5.4 测试覆盖对比

| 测试类型 | Celeborn | Uniffle |
|---------|----------|---------|
| **单元测试** | ✅ 良好 | ✅ 良好 |
| **集成测试** | ✅ 完善 | ✅ 完善 |
| **Benchmark** | ⚠️ 较少公开 | ✅ 详细公开 |
| **压力测试** | ⚠️ 未公开 | ✅ 公开 11200 并发 |
| **混沌测试** | ❌ | ❌ |

---

## 6. 部署与运维对比

### 6.1 部署方式

| 方式 | Celeborn | Uniffle |
|-----|----------|---------|
| **二进制部署** | ✅ | ✅ |
| **Docker** | ✅ | ✅ |
| **Kubernetes** | ✅ Operator | ✅ Operator |
| **Helm Chart** | ✅ | ✅ |

### 6.2 配置复杂度

**Celeborn** 关键配置:
```properties
# Master
ces.master.endpoints=master1:9097,master2:9097,master3:9097

# Worker
ces.worker.endpoints=worker1:9092

# Storage
ces.storage.activeTypes=MEMORY,SSD,HDD
```

**Uniffle** 关键配置:
```properties
# Coordinator
rss.coordinator.server.heartbeat.timeout=30000
rss.coordinator.shuffle.nodes.max=5

# Shuffle Server
rss.storage.type=MEMORY_LOCALFILE_HDFS
rss.server.buffer.capacity=40g
rss.coordinator.quorum=coordinator1:19999,coordinator2:19999
```

**对比**:
| 指标 | Celeborn | Uniffle |
|-----|----------|---------|
| 配置项数量 | ~500 | ~400 |
| 必填配置 | 较少 | 较少 |
| 默认配置 | 合理 | 需调整 Buffer |
| 文档完整性 | 良好 | 良好 |

### 6.3 监控与告警

| 功能 | Celeborn | Uniffle |
|-----|----------|---------|
| **Metrics 格式** | Prometheus | Prometheus |
| **内置 Dashboard** | ✅ | ✅ (Vue.js) |
| **JMX 支持** | ✅ | ✅ |
| **日志级别** | INFO/WARN/ERROR | INFO/WARN/ERROR |
| **Tracing** | ⚠️ 部分支持 | ❌ |

### 6.4 升级与维护

| 操作 | Celeborn | Uniffle |
|-----|----------|---------|
| **滚动升级** | ✅ 支持 | ✅ 支持 |
| **配置热加载** | ⚠️ 部分 | ✅ 支持 |
| **数据迁移** | ⚠️ 复杂 | ⚠️ 复杂 |
| **扩缩容** | ✅ 支持 | ✅ 支持 |

---

## 7. 优缺点总结

### 7.1 Celeborn 优势与劣势

**优势**:
1. **Flink 支持完善** - 对 Flink 的支持优于 Uniffle
2. **云原生友好** - 直接支持 S3/OSS 等云存储
3. **Master HA 成熟** - 基于 Ratis 的强一致 HA
4. **异构存储优化** - 自动适配 HDD/SSD/NVMe
5. **社区活跃** - 阿里巴巴主导，贡献者众多

**劣势**:
1. **Scala 代码** - 对纯 Java 团队学习成本较高
2. **配置复杂** - 功能丰富导致配置项较多
3. **Spark 适配** - Spark 适配不如 Uniffle 深入
4. **性能数据** - 公开的性能 benchmark 较少

### 7.2 Uniffle 优势与劣势

**优势**:
1. **纯 Java 代码** - 易于理解和维护
2. **Spark 适配深入** - 支持 Spark 2.x/3.x 所有版本
3. **双协议支持** - gRPC + Netty，Netty 性能更优
4. **性能数据公开** - 详细的 benchmark 报告
5. **远程合并** - 支持服务端排序合并，减少网络传输
6. **动态配置** - 支持完整的热加载配置

**劣势**:
1. **Flink 不支持** - 目前不支持 Flink
2. **云存储有限** - 仅支持 HDFS，不支持 S3/OSS 直写
3. **Master 无 HA** - Coordinator 无内置强一致 HA
4. **缓冲区激进** - 默认内存配置可能导致 GC 压力

### 7.3 适用场景对比

| 场景 | 推荐 | 原因 |
|-----|------|------|
| **Flink 大作业** | Celeborn | Flink 支持完善 |
| **Spark 生态** | Uniffle | Spark 适配深入 |
| **云原生部署** | Celeborn | S3/OSS 直写支持 |
| **私有化部署** | Uniffle | 纯 Java，维护简单 |
| **超高并发** | Uniffle | Netty 模式稳定性验证 |
| **多租户共享** | Celeborn | 配额管理更完善 |

---

## 8. 选型建议

### 8.1 决策树

```
                    开始选型
                       │
                       ▼
               使用 Flink ?
              /            \
            是              否
            │                │
            ▼                ▼
       选 Celeborn    使用 Spark ?
                      /            \
                    是              否
                    │                │
                    ▼                ▼
              需要云存储 ?       选 Celeborn
             /            \
           是              否
           │                │
           ▼                ▼
      选 Celeborn    团队熟悉 Java ?
                    /            \
                  是              否
                  │                │
                  ▼                ▼
             选 Uniffle      选 Celeborn
```

### 8.2 场景化推荐

**场景 1: 阿里云/腾讯云原生环境**
- **推荐**: Celeborn
- **原因**: 云存储支持好，与云厂商集成深

**场景 2: Spark 大数据平台**
- **推荐**: Uniffle
- **原因**: Spark 支持全面，性能优化到位

**场景 3: 混合计算引擎 (Spark + Flink)**
- **推荐**: Celeborn
- **原因**: 多引擎支持更好

**场景 4: 金融/传统行业**
- **推荐**: Uniffle
- **原因**: 纯 Java 技术栈，易于维护

**场景 5: 超大规模集群 (1000+ 节点)**
- **推荐**: Celeborn
- **原因**: Master HA 成熟，扩展性验证更充分

**场景 6: 高并发实时计算**
- **推荐**: Uniffle
- **原因**: Netty 模式在高并发下更稳定

### 8.3 迁移建议

**从 Celeborn 迁移到 Uniffle**:
1. 评估 Flink 依赖 - 如果有 Flink 作业，需保留 Celeborn
2. 逐步迁移 Spark 作业
3. 统一配置管理
4. 监控双集群并存

**从 Uniffle 迁移到 Celeborn**:
1. 评估云存储需求
2. 评估 Master HA 需求
3. 团队 Scala 学习成本
4. 逐步灰度迁移

### 8.4 未来展望

**Celeborn 发展方向**:
- 性能持续优化
- 云原生能力增强
- 更多计算引擎支持

**Uniffle 发展方向**:
- Flink 支持（已规划）
- 云存储支持（已规划）
- Master HA 增强

---

## 参考资源

- [Celeborn 官方文档](https://celeborn.apache.org/)
- [Uniffle 官方文档](https://uniffle.apache.org/)
- [Celeborn GitHub](https://github.com/apache/celeborn)
- [Uniffle GitHub](https://github.com/apache/incubator-uniffle)
- [Uniffle Benchmark](docs/benchmark_netty_case_report.md)
