# koinos-bridge-validator

local bridge contact:

- address: 0x5FbDB2315678afecb367f032d93F642f64180aa3

command example:

```bash
go run cmd/koinos-bridge-validator/main.go \
    --amqp amqp://guest:guest@localhost:5672/ \
    --validators 1Dd9qtqWTPGvhKRLhyDvPkNsGPK1qNz6Hk:abcd,1GWSnBFJB1fx2Qotb3nx9b2JL9TFB14e2P:efgh
    --reset \
    --no-p2p \
    --eth-rpc http://127.0.0.1:8545/ \
    --eth-contract 0x5FbDB2315678afecb367f032d93F642f64180aa3 \
    --koinos-pk 5KaE1BafyKiYeVk5K1FCJkqcEKxBRJNe9VeWx7iKf6kLTtVAb3k \
    --koinos-contract 1DQzuCcTKacbs9GGScRTU1Hc8BsyARTPqe \
    --token-addresses 19JntSm8pSNETT9aHTwAUHC5RMoaSmgZPJ:0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512
```

```bash
go run cmd/koinos-bridge-validator/main.go \
    --amqp amqp://guest:guest@localhost:5672/ \
    --validators 1Dd9qtqWTPGvhKRLhyDvPkNsGPK1qNz6Hk:abcd,1GWSnBFJB1fx2Qotb3nx9b2JL9TFB14e2P:efgh \
    --reset \
    --eth-rpc http://127.0.0.1:8545/ \
    --eth-contract 0x5FbDB2315678afecb367f032d93F642f64180aa3 \
    --koinos-pk 5KaE1BafyKiYeVk5K1FCJkqcEKxBRJNe9VeWx7iKf6kLTtVAb3k \
    --koinos-contract 1DQzuCcTKacbs9GGScRTU1Hc8BsyARTPqe \
    --token-addresses 19JntSm8pSNETT9aHTwAUHC5RMoaSmgZPJ:0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512
```

```bash
curl --location --request POST 'http://127.0.0.1:8080/' --data-raw '{"id": 2,"jsonrpc": "2.0","method": "bridge.get_ethereum_transaction","params": {"transaction_id": "0x7cba357d8570ae424d2ca2be2df14cdfb55cd9dc557c6171ba1e43a15101f045"}}'
```