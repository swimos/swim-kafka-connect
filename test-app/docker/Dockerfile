FROM ghcr.io/graalvm/graalvm-ce:latest
LABEL maintainer=ajay.gov
WORKDIR /app
COPY libs libs/
ENTRYPOINT ["java", "-Xms128m", "-Xmx128m", "-cp", "/app/libs/*", "swim.app.AppPlane"]
EXPOSE 9001
