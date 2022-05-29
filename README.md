# koinos-bridge-validator

local bridge contact:

- address: 0x5FbDB2315678afecb367f032d93F642f64180aa3

command example:

```bash
go run cmd/koinos-bridge-validator/main.go \
    --reset \
    --no-p2p \
    --eth-rpc http://127.0.0.1:8545/ \
    --eth-contract 0x5FbDB2315678afecb367f032d93F642f64180aa3 \
    --koinos-pk 5KaE1BafyKiYeVk5K1FCJkqcEKxBRJNe9VeWx7iKf6kLTtVAb3k \
    --koinos-contract 1DQzuCcTKacbs9GGScRTU1Hc8BsyARTPqe \
    --token-addresses 19JntSm8pSNETT9aHTwAUHC5RMoaSmgZPJ:0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512
```