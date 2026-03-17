Build the connector and deploy it to the local Trino installation.

```bash
./mvnw clean package -DskipTests && cp -r target/ducklake trino-server-479/plugin/ducklake
```

If Trino is already running, restart it afterward:

```bash
trino-server-479/bin/launcher stop
trino-server-479/bin/launcher run
```
