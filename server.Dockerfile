# Prepare gradle dependency cache
FROM openjdk:14.0.2-slim AS cache

WORKDIR /code

# for i in **/*.gradle; do echo COPY ./$i $(dirname $i)/; done
COPY ./build.gradle ./
COPY ./dataline-api/build.gradle dataline-api/
COPY ./dataline-commons/build.gradle dataline-commons/
COPY ./dataline-config-persistence/build.gradle dataline-config-persistence/
COPY ./dataline-config/build.gradle dataline-config/
COPY ./dataline-db/build.gradle dataline-db/
COPY ./dataline-server/build.gradle dataline-server/
COPY ./dataline-workers/build.gradle dataline-workers/
COPY ./settings.gradle ./
COPY ./.env ./
COPY ./gradlew ./
COPY ./gradle ./gradle

RUN ./gradlew --gradle-user-home=/tmp/gradle_cache clean dependencies --no-daemon

# Build artifact
FROM openjdk:14.0.2-slim AS build

WORKDIR /code

COPY --from=cache /tmp/gradle_cache /home/gradle/.gradle
COPY . /code

# Begin installing singer deps
#RUN mkdir -p /lib/singer
#RUN ./tools/singer/setup_singer_env.buster.sh /lib/singer
# End installing singer deps

RUN apt-get update \
    && apt-get install -y curl \
    && curl -sL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get install -y nodejs

RUN ./gradlew clean build distTar --no-daemon -console rich --stacktrace
RUN ls /code/dataline-server/build/distributions/

# Build final image
FROM openjdk:14.0.2-slim

EXPOSE 8000

WORKDIR /app/dataline-server

# TODO: add data mount instead
RUN mkdir data

COPY --from=build /code/dataline-server/build/distributions/*.tar dataline-server.tar
RUN tar xf dataline-server.tar --strip-components=1

CMD bin/dataline-server
