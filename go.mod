module github.com/roaminroe/koinos-bridge-validator

go 1.16

require (
	github.com/ethereum/go-ethereum v1.10.17
	github.com/koinos/koinos-log-golang v0.0.0-20210621202301-3310a8e5866b
	github.com/koinos/koinos-mq-golang v0.0.0-20220307194511-07a03f6f75f0
	github.com/koinos/koinos-proto-golang v0.2.1-0.20220304200226-d96c9cf694de
	github.com/koinos/koinos-util-golang v0.0.0-20220224193402-85a6df362833
	github.com/spf13/pflag v1.0.3
	google.golang.org/protobuf v1.28.0
)

require (
	github.com/btcsuite/btcd v0.22.1
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/dgraph-io/badger v1.6.2
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/mr-tron/base58 v1.2.0
	go.uber.org/zap v1.17.0
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/koinos/koinos-proto-golang v0.2.1-0.20220304200226-d96c9cf694de => github.com/roaminroe/koinos-proto-golang v0.0.0-20220511165109-9d11027e06ed
