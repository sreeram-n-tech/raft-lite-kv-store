FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kvnode ./cmd/kvnode

FROM alpine:latest
RUN apk add --no-cache curl bash
WORKDIR /app
COPY --from=builder /app/kvnode .
ENTRYPOINT ["./kvnode"]
