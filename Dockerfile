# Stage 1: Build the plugin
FROM eclipse-temurin:25-jdk AS build
WORKDIR /build
COPY pom.xml mvnw ./
COPY .mvn .mvn
# Cache Maven dependencies
RUN ./mvnw dependency:go-offline -q || true
COPY src src
RUN ./mvnw clean package -DskipTests -q

# Stage 2: Runtime
FROM trinodb/trino:479
COPY --from=build /build/target/ducklake /usr/lib/trino/plugin/ducklake
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
USER root
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
USER trino
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
