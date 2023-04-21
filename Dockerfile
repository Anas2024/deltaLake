# Build stage
FROM maven:3.8.3-openjdk-17 AS build
WORKDIR /home/app
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src/ /home/app/src/
RUN mvn -B clean package
USER 1001

# Package stage
FROM adoptopenjdk:17-jre-hotspot
LABEL maintainer="Anas AIT RAHO <anas.aitraho@gmail.com>"
LABEL version="1.0"
LABEL description="Une description de l'application"
WORKDIR /app
COPY --from=build /home/app/target/deltalake-spark-minio.jar /app/deltalake-spark-minio.jar
EXPOSE 8080
CMD ["java", "-jar", "/app/chat-gpt-app.jar"]