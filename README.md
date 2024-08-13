# Jaeger-remote-storage

基于 jaeger 1.59 拓展，参考[jaeger-clickhouse](https://github.com/jaegertracing/jaeger-clickhouse)，支持clickhouse作为后端存储, 同时兼容原来的后端存储

## 构建

```bash
docker build -t apo-jaeger-remote-storage:latest -f cmd/remote-storage/Dockerfile-APO .
```

## 部署

```bash
docker run -e SPAN_STORAGE_TYPE=clickhouse -e CLICKHOUSE_CONFIG=/app/config.yaml apo-jaeger-remote-storage:latest
```
## 配置
clickhouse后端存储配置文件[config.yaml](https://github.com/CloudDetail/jaeger/blob/apo-1.59/cmd/remote-storage/config.yaml)