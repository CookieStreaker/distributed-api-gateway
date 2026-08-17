# =============================================================================
# Multi-Stage Dockerfile for the Distributed API Gateway
# =============================================================================
# Stage 1: Compile the Go binary with all dependencies
# Stage 2: Copy into a minimal Alpine image (~15MB total)
# =============================================================================

# --- Stage 1: Build ---
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy dependency files first (Docker layer caching optimization)
COPY go.mod go.sum ./
RUN go mod download

# Copy all source code
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
