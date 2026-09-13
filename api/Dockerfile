# syntax=docker/dockerfile:1
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go vet ./... \
    && CGO_ENABLED=0 go test ./... \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/api .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/api /api
VOLUME /data
EXPOSE 8080
ENV DATA_DIR=/data ADDR=:8080
ENTRYPOINT ["/api"]
