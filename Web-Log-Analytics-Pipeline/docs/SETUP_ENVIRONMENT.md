# Environment Setup Guide
## Project #53: Batch MapReduce Clickstream Log Analysis

This document describes the environment prerequisites, setup commands, and verification procedures for compiling and running the pure Hadoop MapReduce pipeline.

---

## 1. Prerequisites & Host Findings

An inspection of the local environment revealed:
- **Windows Host**: Has OpenJDK 21 installed (`21.0.11`), but lacks Maven (`mvn: command not found`).
- **WSL2 (Ubuntu 22.04 / Linux)**: Installed (`wsl --list -v`), but currently lacks OpenJDK 11 and Maven.
- **Docker Desktop**: Installed (`29.7.2`), but the daemon is currently stopped.

Per [CLAUDE.md §5](file:///d:/BDA_LAB/docs/CLAUDE.md#L92), the required versions are:
- **Java**: OpenJDK 11 (strict requirement for Hadoop 3.3.6 compatibility).
- **Maven**: 3.9+ (or 3.6+).
- **Hadoop**: 3.3.6 (client and cluster).
- **Python**: 3.10+ (for ingestion, golden verification, and Streamlit dashboard).

---

## 2. Recommended Setup: WSL2 Ubuntu

Because the production batch execution relies on Linux `cron`, `flock`, and Bash scripts (`nightly_batch.sh`), WSL2 Ubuntu is the primary recommended runtime.

### Step 1: Open WSL2 Terminal
```bash
wsl -d Ubuntu
```

### Step 2: Run Setup Script or Install Packages
```bash
# Run automated setup:
bash scripts/setup_environment.sh

# Or install manually:
sudo apt update
sudo apt install -y openjdk-11-jdk maven python3 python3-pip util-linux
```

### Step 3: Verify Tools
```bash
java -version    # Must show OpenJDK 11.x
mvn -version     # Must show Maven 3.x
python3 --version # Must show Python 3.10+
```

---

## 3. Building the Java MapReduce Application

Once OpenJDK 11 and Maven are available:
```bash
# Run unit and integration tests with JaCoCo verification
mvn clean verify

# Build application JAR: target/clickstream-mr-1.0.jar
mvn clean package -DskipTests
```

---

## 4. Starting the Hadoop & YARN Cluster (Docker)

Once Docker Desktop is started on the Windows host:
```bash
# 1. Start HDFS NameNode, DataNode, YARN ResourceManager, NodeManager & JobHistoryServer
docker compose up -d

# 2. Check cluster health
docker compose ps

# 3. Access Web UIs:
#    - HDFS NameNode: http://localhost:19870
#    - YARN ResourceManager: http://localhost:8088
#    - JobHistoryServer: http://localhost:19888
```
