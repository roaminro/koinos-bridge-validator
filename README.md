# WARNING: THE CODE IN THIS REPOSITORY HAS NOT BEEN TESTED NOR AUDITED. IT IS NOT INTENDED FOR PRODUCTION

## koinos-bridge-validator

### Golang

command example:

Start a node

```bash
go run cmd/koinos-bridge-validator/main.go -d "$(pwd)/node_test"
```

`-d` defines the path of your config file

### Docker Compose

Set the correct ports in `config.yaml` and create a `.env` file that defines a PORTS variable which will be used in `docker-compose.yml`.

```bash
PORTS="5005:5005"
```

### Validator Queries

Query transactions status

```bash
curl -X GET \
  'http://localhost:3000/GetEthereumTransaction?TransactionId=0xc4400da5eb03fec6eb0450d1e02b694ea049d103e85ed0d10d568df2ee7800ad' \
  --header 'Accept: */*'

curl -X GET \
  'http://localhost:3000/GetKoinosTransaction?TransactionId=0x12207638d5874c57ff042d9268927f79c8cd151d3ff0f94b2e366d154cc1c2d9807f' \
  --header 'Accept: */*'
```

## Config

Create a `config.yaml` file.

```yaml
bridge:
  reset: false
  api-url: 
  ethereum-rpc: 
  ethereum-pk: 
  ethereum-contract: 
  ethereum-max-blocks-stream: 
  ethereum-block-start: (optional)
  koinos-rpc: 
  koinos-pk: 
  koinos-contract: 
  koinos-block-start: (optional)
  validators:
    val1:
      ethereum-address: ""
      koinos-address: 
      api-url: 
    val2:
      ethereum-address: ""
      koinos-address: 
      api-url: 
  tokens:
    tKoin:
      ethereum-address: ""
      koinos-address: 
    wETH:
      ethereum-address: ""
      koinos-address: 
```
