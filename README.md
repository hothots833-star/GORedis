# GORedis
GORedis — lightweight in-memory key-value store inspired by Redis, written in Go.  

# Features:  
- SET / GET / DEL operations via TCP
- Persistent storage with MessagePack serialization
- Atomic file writes (temp file + rename)
- Concurrent access with RWMutex
- Multiple simultaneous connections
- Graceful shutdown with data saving
- TTL support with lazy expiration
