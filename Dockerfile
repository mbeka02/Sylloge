FROM golang:1.26.0-bookworm@sha256:2a0ba12e116687098780d3ce700f9ce3cb340783779646aafbabed748fa6677c AS development
ENV GOTOOLCHAIN=local
WORKDIR /app
COPY go.mod go.sum ./
RUN test "$(go env GOVERSION)" = "go$(sed -n 's/^go //p' go.mod)"
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY configs ./configs
RUN mkdir -m 0700 /app/data && go build -trimpath -o /usr/local/bin/sylloge ./cmd
CMD ["/usr/local/bin/sylloge"]

FROM development AS build
RUN CGO_ENABLED=0 go build -trimpath -o /out/sylloge ./cmd

FROM scratch AS runtime
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/sylloge /sylloge
COPY --from=build /app/configs /configs
COPY --from=build --chown=65532:65532 /app/data /data
USER 65532:65532
WORKDIR /
ENTRYPOINT ["/sylloge"]
