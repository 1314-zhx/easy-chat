#!/bin/bash
# 下载 Jaeger MongoDB Storage Plugin

PLUGIN_DIR="$(dirname "$0")/jaeger-plugin"
mkdir -p "$PLUGIN_DIR"

echo "下载 Jaeger MongoDB Storage Plugin..."

# 从 GitHub releases 下载 (Linux amd64)
if command -v wget &> /dev/null; then
    wget -q --show-progress -O /tmp/jaeger-storage-mongodb.tar.gz \
        https://github.com/jaegertracing/jaeger-storage-mongodb/releases/download/v0.0.1/jaeger-storage-mongodb_0.0.1_linux_amd64.tar.gz
elif command -v curl &> /dev/null; then
    curl -L -o /tmp/jaeger-storage-mongodb.tar.gz \
        https://github.com/jaegertracing/jaeger-storage-mongodb/releases/download/v0.0.1/jaeger-storage-mongodb_0.0.1_linux_amd64.tar.gz
else
    echo "需要 wget 或 curl 来下载插件"
    exit 1
fi

# 解压到插件目录
tar -xzf /tmp/jaeger-storage-mongodb.tar.gz -C "$PLUGIN_DIR"
rm /tmp/jaeger-storage-mongodb.tar.gz

echo "插件已安装到: $PLUGIN_DIR"
ls -la "$PLUGIN_DIR"
