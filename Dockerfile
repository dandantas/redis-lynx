FROM golang:1.24.1-alpine

# Set the current working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to the workspace
# Copy go.mod and go.sum files to the workspace if found
COPY go.mod ./


# Download all dependencies
RUN go mod download

RUN go install github.com/air-verse/air@latest

# Copy the source from the current directory to the workspace
COPY . .

# Build the Go app
RUN go build -o ./tmp/main ./cmd/api/main.go

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["air"]
