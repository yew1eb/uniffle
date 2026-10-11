# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

Apache Uniffle is a high-performance remote shuffle service for distributed computing frameworks (Spark, MapReduce, Tez). It follows a three-tier architecture: **Coordinator** → **Shuffle Server** → **Client Libraries**.

## Build Commands

```bash
# Build (skip tests) - default Hadoop 2.8 + Spark 3
./mvnw -DskipTests clean package

# Build with specific Spark profile
./mvnw -DskipTests clean package -Pspark3.5
./mvnw -DskipTests clean package -Pspark2

# Build distribution tarball
./build_distribution.sh
./build_distribution.sh --spark3-profile 'spark3.2' --hadoop-profile 'hadoop3.2'
```

## Testing

```bash
# Run all unit tests
./mvnw test -Dtest='!org.apache.uniffle.test.**'

# Run unit tests with a specific profile
./mvnw test -Pspark3 -Dtest='!org.apache.uniffle.test.**'

# Run a single test class
./mvnw test -Dtest=CoordinatorConfTest

# Run a specific test method
./mvnw test -Dtest=CoordinatorConfTest#testMethod

# Run integration tests
./mvnw test -Dtest='org.apache.uniffle.test.**'
```

## Code Quality

```bash
# Auto-fix code formatting (run before committing)
./mvnw spotless:apply -Pspark3 -Pspark2 -Ptez -Pmr -Phadoop2.8 -Pdashboard

# Validate formatting
./mvnw spotless:check

# Checkstyle
./mvnw checkstyle:check

# SpotBugs static analysis
./mvnw test-compile spotbugs:check

# License header check
./mvnw apache-rat:check
```

Code style: 2-space indentation, max 2000 lines per file (enforced by `checkstyle.xml`).

## Architecture

### Three-Tier Design

```
Client (Spark/MR/Tez) ──gRPC──► Coordinator ──gRPC──► Shuffle Servers
                                                              │
                                                         Storage Layer
                                                    (Memory / LocalDisk / HDFS)
```

**Coordinator** (`coordinator/`): Stateful service that tracks shuffle server health, assigns servers to jobs, and manages dynamic client configuration and access control.

**Shuffle Server** (`server/`): Receives shuffle write blocks from clients, buffers in memory, then flushes to local disk and/or HDFS. Serves read requests. Supports storage types: `MEMORY`, `MEMORY_LOCALFILE`, `MEMORY_LOCALFILE_HDFS`.

**Client Libraries** (version-specific modules):
- `client/` — shared client abstractions
- `internal-client/` — internal RPC client used by both coordinator and server
- `client-spark/common|spark2|spark3` — Spark shuffle manager integration
- `client-mr/core|hadoop2.8|hadoop3.2` — MapReduce integration
- `client-tez/` — Apache Tez integration

### Key Modules

| Module | Purpose |
|--------|---------|
| `proto/` | Protobuf definitions for all gRPC services |
| `common/` | Shared config, metrics, RPC utilities, compression codecs |
| `server-common/` | Utilities shared between server and coordinator |
| `storage/` | Pluggable storage backends (local file, HDFS, S3) |
| `integration-test/` | End-to-end tests requiring a live cluster |
| `cli/` | Admin CLI tools |
| `dashboard/` | Optional Vue.js web UI |

### Communication

All inter-service communication uses **gRPC + Protobuf**. Protobuf sources in `proto/src/main/proto/` are compiled during the build. Network I/O is handled by **Netty**.

### Build Profiles

Profiles for framework versions are mutually exclusive per dimension:
- Spark: `spark2`, `spark2.3`, `spark3`, `spark3.2`, `spark3.3`, `spark3.4`, `spark3.5`, `spark3.5-scala2.13`
- Hadoop/MR: `mr`, `hadoop2.8`, `hadoop3.2`
- Tez: `tez`, `tez-hadoop3.2`
- Other: `dashboard`, `kubernetes`

Client JARs for Spark/MR/Tez are **shaded** to avoid dependency conflicts with the compute framework classpath.
