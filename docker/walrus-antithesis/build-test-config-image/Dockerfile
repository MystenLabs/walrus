FROM debian:bookworm-slim AS setup

RUN apt update
RUN apt install python3 python3-pip -y

WORKDIR /

RUN echo "SUI_RUST_LOG=$SUI_RUST_LOG" >> /.env

# TODO(zhewu): update to use env variables
RUN echo "WALRUS_IMAGE_NAME=walrus-service:walrus-service-zhe" >> /.env
RUN echo "WALRUS_PLATFORM=linux/amd64" >> /.env
RUN echo "SUI_IMAGE_NAME=mysten/sui-tools:mainnet-v1.40.3" >> /.env

COPY ./docker-compose.yaml /docker-compose.yaml
COPY ./files /files

FROM scratch

COPY --from=setup /docker-compose.yaml /docker-compose.yaml
COPY --from=setup /.env /.env
COPY --from=setup /files /files
