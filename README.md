# koinos-bridge-validator

local bridge contact:
- address: 0x5FbDB2315678afecb367f032d93F642f64180aa3
- logs topic: 0xb656d86127d7832fca2f3d9b253a58f55cec150b88b33cf54c8c514ab7ea623e

go run cmd/koinos-bridge-validator/main.go --eth-rpc http://127.0.0.1:8545/ --eth-contract 0x5FbDB2315678afecb367f032d93F642f64180aa3 --eth-logs-topic 0xb656d86127d7832fca2f3d9b253a58f55cec150b88b33cf54c8c514ab7ea623e --reset

go run cmd/koinos-bridge-validator/main.go --eth-rpc http://127.0.0.1:8545/ --eth-contract 0x5FbDB2315678afecb367f032d93F642f64180aa3 --eth-logs-topic 0xb656d86127d7832fca2f3d9b253a58f55cec150b88b33cf54c8c514ab7ea623e --no-p2p