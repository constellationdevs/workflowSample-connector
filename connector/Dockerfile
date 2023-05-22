FROM openjdk:17-alpine
VOLUME /d/tmp
ARG JAR_FILE
ADD ${JAR_FILE} app.jar 
EXPOSE 9154
RUN apk --no-cache add curl
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]