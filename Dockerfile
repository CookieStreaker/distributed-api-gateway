FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gateway .

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/gateway .
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/dashboard/ ./dashboard/

EXPOSE 8080 8081

CMD ["./gateway"]
