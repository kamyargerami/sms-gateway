FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod ./

RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /worker ./cmd/worker

FROM alpine:latest AS api
WORKDIR /app
COPY --from=builder /api /api
EXPOSE 8080
CMD ["/api"]

FROM alpine:latest AS worker
WORKDIR /app
COPY --from=builder /worker /worker
CMD ["/worker"]
