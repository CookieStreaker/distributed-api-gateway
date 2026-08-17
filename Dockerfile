FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a statically-linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gateway .

# --- Stage 2: Runtime ---
FROM alpine:latest

WORKDIR /app

# Install CA certificates for HTTPS upstream support
RUN apk --no-cache add ca-certificates

# Copy the compiled binary from the builder stage
COPY --from=builder /app/gateway .

# Copy configuration and static assets
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/dashboard/ ./dashboard/

# Expose the gateway proxy port and admin dashboard port
EXPOSE 8080 8081

# Run the gateway
CMD ["./gateway"]
