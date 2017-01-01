FROM java:8

RUN mkdir -p /app/
ADD kinesis-firehose.jar /app/app.jar

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s \
  CMD curl -f http://localhost:8080/health || exit 1
CMD ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app/app.jar"]
