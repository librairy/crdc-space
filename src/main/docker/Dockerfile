FROM anapsix/alpine-java:8u162b12_server-jre
#FROM frolvlad/alpine-oraclejdk8:slim
VOLUME /tmp
ADD crdc-space.jar /app.jar
ENTRYPOINT exec java $JAVA_OPTS -server -jar /app.jar
