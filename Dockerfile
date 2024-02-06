FROM alpine:3.19
RUN apk add --no-cache openjdk21
COPY ./app/build/libs/app.jar ./app.jar
CMD java -jar app.jar