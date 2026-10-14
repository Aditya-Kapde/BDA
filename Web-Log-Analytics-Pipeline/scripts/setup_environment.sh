#!/usr/bin/env bash
# scripts/setup_environment.sh — Sets up OpenJDK 11, Maven 3.9, and Python dependencies in WSL2 / Ubuntu
set -euo pipefail

echo "=== Clickstream Log Analysis: Environment Setup ==="

# Check OS
if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    echo "Detected OS: $NAME ($VERSION)"
else
    echo "Warning: Not running on a standard Linux distribution."
fi

echo "Updating apt repositories..."
sudo apt-get update -y

echo "Installing OpenJDK 11, Maven, Python 3, and utilities..."
sudo apt-get install -y \
    openjdk-11-jdk \
    maven \
    python3 \
    python3-pip \
    curl \
    util-linux

# Configure JAVA_HOME
JAVA_11_PATH="/usr/lib/jvm/java-11-openjdk-amd64"
if [[ -d "$JAVA_11_PATH" ]]; then
    export JAVA_HOME="$JAVA_11_PATH"
    echo "export JAVA_HOME=$JAVA_11_PATH" >> ~/.bashrc
    echo "JAVA_HOME set to $JAVA_HOME"
fi

echo "Verifying installed tools:"
java -version
mvn -version
python3 --version

echo "=== Environment setup complete ==="
