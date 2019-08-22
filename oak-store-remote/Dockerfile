FROM openjdk:11

RUN mkdir /app
ARG JAR_FILE
COPY target/${JAR_FILE} /app/oak-store-remote-server.jar
WORKDIR /app

VOLUME /app/segmentstore
VOLUME /app/datastore

CMD ["java", "-Xmx4g", "-jar", "oak-store-remote-server.jar"]
