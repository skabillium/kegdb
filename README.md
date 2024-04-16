# KegDB

Keg is a disk based key-value database system based on
[BitCask by Riak](https://riak.com/assets/bitcask-intro.pdf). This is a database made for educational
purposes and not intended for production use.

## What is BitCask?

BitCask is a log-structured storage engine aiming to provide low latency writes and store 
datasets larger than memory. It is designed around an append-only write file and multiple
read-only files to store the keys. An index is also used to map the keys to their corresponding
files. For more information about the BitCask system you can take a look at the [original paper](https://riak.com/assets/bitcask-intro.pdf)

## Setting up the project

```sh
make install
make start
```
This will start the server at the defaul port `5678`, you can take a look at the command line
options with `make help`.

## Interacting with the server

The server uses the [RESP protocol]() for communication since it is lightweight and easy to implement
although it does not support the same commands as Redis.

The following commands are supported:
- `PUT {key} {value}`, set a key to a value
- `GET {key}`, get a key
- `KEYS`, list keys
- `DEL {key}`, remove key
- `INDEX`, re-index the keys map from all the data files
- `MERGE`, compact all datafiles and update indexes
- `QUIT`, quit session

To send the commands to the server you can either use a redis client in the language of your choice 
or connect via TCP and send the commands directly. You can use `telnet` (Windows/Linux) or 
`netcat` (MacOS) like this:
```
nc localhost 5678
> put message "Hello there!"
```
