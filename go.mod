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
	github.com/StackExchange/wmi v0.0.0-20180116203802-5d049714c4a6 // indirect
	github.com/btcsuite/btcd v0.22.1 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.1.2 // indirect
	github.com/deckarep/golang-set v1.8.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/go-ole/go-ole v1.2.1 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-multihash v0.1.0 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	github.com/tklauser/go-sysconf v0.3.5 // indirect
	github.com/tklauser/numcpus v0.2.2 // indirect
	go.uber.org/zap v1.17.0
	golang.org/x/crypto v0.0.0-20210322153248-0c34fe9e7dc2 // indirect
	golang.org/x/sys v0.0.0-20210816183151-1e6c022a8912 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/natefinch/npipe.v2 v2.0.0-20160621034901-c1b8fa8bdcce // indirect
	gopkg.in/yaml.v2 v2.4.0
	lukechampine.com/blake3 v1.1.6 // indirect
)

replace github.com/koinos/koinos-proto-golang v0.2.1-0.20220304200226-d96c9cf694de => github.com/roaminroe/koinos-proto-golang v0.0.0-20220511165109-9d11027e06ed
