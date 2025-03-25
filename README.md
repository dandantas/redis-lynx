# Redis Lynx

Redis Lynx is a real-time event propagation service that listens to Redis keyspace notifications and broadcasts them to WebSocket clients.

## Features

- Connects to Redis and listens for keyspace events (SET, EXPIRED, DEL)
- Broadcasts events to connected WebSocket clients in real-time
- Configurable via environment variables or .env file
- Built with Go's concurrency for high performance

## Requirements

- Go 1.24 or higher
- Redis server (with keyspace notifications enabled)

## Installation

```bash
# Clone the repository
git clone https://github.com/dandantas/redis-lynx.git
cd redis-lynx

# Build and run the application with air
docker compose up -d
```

## Usage

### Configuration

Redis Lynx can be configured using environment variables or a `.env` file:

```
# Redis configuration
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=your_password
REDIS_DB=0

# WebSocket configuration
WEBSOCKET_PORT=8080

# Application configuration
DEBUG=false
```

### Running with Docker

The easiest way to run Redis Lynx is with Docker Compose:

```bash
docker-compose up -d
```

This will start both Redis and Redis Lynx services.

### Running Manually

```bash
# Start the application
./redis-lynx
```

## WebSocket Client Example

Connect to the WebSocket server at `ws://localhost:8080/ws` to receive events:

```javascript
const socket = new WebSocket('ws://localhost:8080/ws');

socket.onopen = () => {
  console.log('Connected to Redis Lynx');
};

socket.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Redis event:', data);
};

socket.onclose = () => {
  console.log('Disconnected from Redis Lynx');
};
```

## Event Format

Events are broadcasted as JSON objects with the following structure:

```json
{
  "type": "set",
  "key": "user:123",
  "value": "John Doe",
  "ttl": 3600000000000,
  "timestamp": "2023-01-01T12:00:00Z"
}
```

- `type`: The event type (set, expired, del)
- `key`: The Redis key that triggered the event
- `value`: The value of the key (only for `set` events)
- `ttl`: The time-to-live of the key in nanoseconds (only for `set` events with expiry)
- `timestamp`: When the event occurred

## License

MIT 