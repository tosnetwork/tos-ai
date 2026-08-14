# syntax=docker/dockerfile:1.7
FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241

COPY --chown=0:0 go /usr/local/go
RUN ln -s /usr/local/go/bin/go /usr/local/bin/go

ENV PATH=/usr/local/go/bin
WORKDIR /
