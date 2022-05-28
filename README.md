# koinos-bridge-validator

local bridge contact:

- address: 0x5FbDB2315678afecb367f032d93F642f64180aa3
- logs topic: 0xb656d86127d7832fca2f3d9b253a58f55cec150b88b33cf54c8c514ab7ea623e

command example:

go run cmd/koinos-bridge-validator/main.go \
    --reset \
    --no-p2p \
    --eth-rpc http://127.0.0.1:8545/ \
    --eth-contract 0x5FbDB2315678afecb367f032d93F642f64180aa3 \
    --eth-logs-topic 0x4c21824fb18b7a1c065b8a879a8952f7513e394fdc44b597dd51ea954cf37bf9 \
    --koinos-pk 5KaE1BafyKiYeVk5K1FCJkqcEKxBRJNe9VeWx7iKf6kLTtVAb3k \
    --koinos-contract 1DQzuCcTKacbs9GGScRTU1Hc8BsyARTPqe \
    --token-addresses 19JntSm8pSNETT9aHTwAUHC5RMoaSmgZPJ:0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512