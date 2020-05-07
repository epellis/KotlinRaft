FROM gradle:6.4.0-jdk AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build --no-daemon

FROM openjdk:15-jdk-alpine
COPY ./build/libs/KotlinRaft-1.0-SNAPSHOT-all.jar /tmp
WORKDIR /tmp
ENTRYPOINT ["java", "-jar", "KotlinRaft-1.0-SNAPSHOT-all.jar"]