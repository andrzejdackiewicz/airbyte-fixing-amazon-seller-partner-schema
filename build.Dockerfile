###################
# Build artifacts #
###################
FROM openjdk:14.0.2-slim AS build

WORKDIR /code

# Cache Gradle executable
COPY ./gradlew .
COPY ./gradle ./gradle
RUN ./gradlew build --no-daemon

# Copy code, etc.
COPY . /code

# Create distributions
RUN ./gradlew clean distTar build -x test --no-daemon -g /home/gradle/.gradle

######################
# Build webapp image #
######################
FROM nginx:1.19-alpine as webapp

EXPOSE 80

COPY --from=build /code/dataline-webapp/build /usr/share/nginx/html

######################
# Build server image #
######################
FROM openjdk:14.0.2-slim AS server

EXPOSE 8000

ENV WAIT_VERSION=2.7.2
ENV APPLICATION dataline-server

WORKDIR /app

# Install wait
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/${WAIT_VERSION}/wait wait
RUN chmod +x wait

COPY --from=build /code/dataline-config-init/src/main/resources/config data
COPY --from=build /code/${APPLICATION}/build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "./wait && bin/${APPLICATION}"]

#########################
# Build scheduler image #
#########################
FROM openjdk:14.0.2-slim AS scheduler

ENV WAIT_VERSION=2.7.2
ENV APPLICATION dataline-scheduler

WORKDIR /app

# Install wait
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/${WAIT_VERSION}/wait wait
RUN chmod +x wait

COPY --from=build /code/dataline-config-init/src/main/resources/config data
COPY --from=build /code/${APPLICATION}/build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "./wait && bin/${APPLICATION}"]
