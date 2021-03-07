FROM openjdk:8-jdk-alpine
ARG JAR_FILE=target/uber-kafkaConsumer-1.0-SNAPSHOT.jar
ARG KRB5=krb5.conf
COPY ${KRB5} /etc/krb5.conf
COPY ${JAR_FILE} app.jar
EXPOSE 9000 9092 2181 22
ENTRYPOINT ["java","-jar","/app.jar"]
