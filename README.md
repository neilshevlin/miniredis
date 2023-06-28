# miniredis
A minimal implementation of an in memory key value store. This is a toy project to learn more about redis and C++. It is not intended to be used in production.

## Build
Or use some other build system of your choice.
```
mkdir build
cd build
cmake ..
make
```

## Run
```
./miniredis
```

## Folder structure
The main entry point is `server.cpp` This contains the main level abstraction and implementation of the server. 
The other files in the folder are generic implementations of different data structures in use in the server.

## Server
- `main()` gets back a file descriptor from the kernel and sets up a listening socket connection with an addr. 
- main then handles incoming connections. When a connection is accepted, a new thread is spawned to handle the connection.
- When a connection is made, it will contain a header and some data. The header contains the length of the data.
- The data will have some type of request. The server will parse the request and send back a response.

## Commands
The server implements the following commands. The commands are case sensitive.

- `keys` - returns all keys in the store
- `get <key>` - returns the value of the key
- `set <key> <value>` - sets the value of the key
- `del <key>` - deletes the key
- `pexpire <key> <time>` - sets the expiration time of the key in milliseconds
- `pttl <key>` - returns the time to live of the key in milliseconds
- `zadd <key> <score> <value>` - adds a value to a sorted set with a score
- `zrem <key> <value>` - removes a value from a sorted set
- `zscore <key> <value>` - returns the score of a value in a sorted set
- `zquery <key> <min> <max> <offset> <count>` - returns a range of values in a sorted set

These are only a few commands, but implements the basic functionality you would expect in redis.



