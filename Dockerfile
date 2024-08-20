FROM rust:1.80.1
ADD . /app
WORKDIR /app
RUN cargo install --path .
WORKDIR /data
ENTRYPOINT ["s3-manifest"]
