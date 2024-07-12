# Use a Golang 1.19 base image for the builder stage
FROM golang:1.19-alpine AS builder

WORKDIR /app

# Copy the Go module and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code into the container
COPY main.go ./

# Build the Go application
RUN go build -o main .

# Use a smaller base image for the final container
FROM alpine:latest
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Command to run the executable
CMD ["./main"]