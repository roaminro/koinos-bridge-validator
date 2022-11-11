# koinos-bridge-validator

command example:

Start a node
```bash
go run cmd/koinos-bridge-validator/main.go -d "$(pwd)/node_test"
```

Query transactions status
```bash
curl -X GET \
  'http://localhost:3000/GetEthereumTransaction?TransactionId=0xc4400da5eb03fec6eb0450d1e02b694ea049d103e85ed0d10d568df2ee7800ad' \
  --header 'Accept: */*' \

curl -X GET \
  'http://localhost:3000/GetKoinosTransaction?TransactionId=0x12207638d5874c57ff042d9268927f79c8cd151d3ff0f94b2e366d154cc1c2d9807f' \
  --header 'Accept: */*' \
```