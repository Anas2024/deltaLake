# Build stage
FROM maven:3.6-openjdk-17-slim AS build
WORKDIR /home/app
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src/ /home/app/src/
RUN mvn -B clean package
USER 1001

# Package stage #
FROM openjdk:20-ea-17-jdk
ENV JAVA_TOOL_OPTIONS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
LABEL maintainer="Anas AIT RAHO <anas.aitraho@gmail.com>"
LABEL version="1.0"
LABEL description="Une description de l'application"
WORKDIR /app
COPY --from=build /home/app/target/deltalake-spark-minio.jar /app/deltalake-spark-minio.jar
EXPOSE 8080
CMD ["java", "-Xmx512m", "-jar", "/app/deltalake-spark-minio.jar"]
