FROM rustlang/rust:nightly-buster as builder

RUN apt-get update -y && \
    apt install upx cmake libssl-dev libsasl2-dev wget \
    clang -y

ADD .. /opt/src
WORKDIR /opt/src
RUN cargo build --release --examples && \
    ls -al /opt/src/target/release && \
    chmod +x /opt/src/target/release/examples/task && \
    chmod +x /opt/src/target/release/examples/web && \
    upx /opt/src/target/release/examples/task && \
    upx /opt/src/target/release/examples/web


# https://github.com/GoogleContainerTools/distroless/tree/main/cc
FROM gcr.io/distroless/cc
COPY --from=builder /opt/src/target/release/examples/task /usr/local/bin/task
COPY --from=builder /opt/src/target/release/examples/web /usr/local/bin/web

EXPOSE 8000

ARG GIT_REFERENCE
ENV GIT_REFERENCE=$GIT_REFERENCE

WORKDIR /opt/src
CMD ["web"]