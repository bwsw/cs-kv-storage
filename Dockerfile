FROM openjdk:8-alpine

WORKDIR /opt/cs-kv-storage

ARG APP_PATH=https://oss.sonatype.org/service/local/artifact/maven/redirect?r=snapshots&g=com.bwsw&a=cs-kv-storage_2.12&c=jar-with-dependencies&v=1.0.2-SNAPSHOT

EXPOSE 8080

VOLUME ["/var/log/cs-kv-storage"]

ADD ["$APP_PATH", "cs-kv-storage.jar"]

ENTRYPOINT ["java", "-Dconfig.file=application.conf", "-jar", "cs-kv-storage.jar"]
