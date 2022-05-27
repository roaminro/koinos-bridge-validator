package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v3"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	prpc "github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/p2p"
	rpcplugin "github.com/koinos/koinos-proto-golang/koinos/rpc/plugin"
	"github.com/roaminroe/koinos-bridge-validator/internal/store"
	"github.com/roaminroe/koinos-bridge-validator/internal/util"
	"github.com/roaminroe/koinos-bridge-validator/proto/build/github.com/roaminroe/koinos-bridge-validator/bridge_pb"
	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	log "github.com/koinos/koinos-log-golang"
	koinosUtil "github.com/koinos/koinos-util-golang"

	flag "github.com/spf13/pflag"
)

const (
	basedirOption          = "basedir"
	amqpOption             = "amqp"
	instanceIDOption       = "instance-id"
	logLevelOption         = "log-level"
	resetOption            = "reset"
	ethRPCOption           = "eth-rpc"
	ethContractOption      = "eth-contract"
	ethBlockStartOption    = "eth-block-start"
	ethPKOption            = "eth-pk"
	koinosContractOption   = "koinos-contract"
	koinosBlockStartOption = "koinos-block-start"
	koinosPKOption         = "koinos-pk"
	validatorsOption       = "validators"
	tokensAddressesOption  = "token-addresses"
)

const (
	basedirDefault          = ".koinos-bridge"
	amqpDefault             = "amqp://guest:guest@localhost:5672/"
	instanceIDDefault       = ""
	logLevelDefault         = "info"
	resetDefault            = false
	ethRPCDefault           = "http://127.0.0.1:8545/"
	ethBlockStartDefault    = "0"
	koinosBlockStartDefault = "0"
)

const (
	pluginName = "plugin.bridge"
	appName    = "koinos_bridge"
	logDir     = "logs"
)

func main() {
	var baseDir = flag.StringP(basedirOption, "d", basedirDefault, "the base directory")
	var amqp = flag.StringP(amqpOption, "a", "", "AMQP server URL")
	var reset = flag.BoolP("reset", "r", false, "reset the database")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this service")
	logLevel := flag.StringP(logLevelOption, "v", logLevelDefault, "The log filtering level (debug, info, warn, error)")

	ethRPC := flag.StringP(ethRPCOption, "e", ethRPCDefault, "The url of the Ethereum RPC")
	ethContract := flag.StringP(ethContractOption, "c", "", "The address of the Ethereum bridge contract")
	ethBlockStart := flag.StringP(ethBlockStartOption, "t", ethBlockStartDefault, "The block from where to start the Ethereum blockchain streaming")
	ethPK := flag.StringP(ethPKOption, "p", "", "The private key to use to sign Ethereum related transfers")

	koinosContract := flag.StringP(koinosContractOption, "k", "", "The address of the Koinos bridge contract")
	koinosBlockStart := flag.StringP(koinosBlockStartOption, "o", koinosBlockStartDefault, "The block from where to start the Koinos blockchain streaming")
	koinosPK := flag.StringP(koinosPKOption, "w", "", "The private key to use to sign Koinos related transfers")

	validators := flag.StringSliceP(validatorsOption, "v", []string{}, "Koinos Addresses of the validators")
	tokenAddressesArr := flag.StringSliceP(tokensAddressesOption, "v", []string{}, "Addresses of the tokens supported by the bridge in the foram KOIN_TOKEN_ADDRRESS1:ETH_TOKEN_ADDRRESS1")

	flag.Parse()

	var err error
	*baseDir = koinosUtil.InitBaseDir(*baseDir)
	if err != nil {
		panic(fmt.Sprintf("Could not initialize baseDir: %s", *baseDir))
	}

	yamlConfig := util.InitYamlConfig(*baseDir)

	*amqp = koinosUtil.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.KoinosBridge, yamlConfig.Global)
	*logLevel = koinosUtil.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.KoinosBridge, yamlConfig.Global)
	*instanceID = koinosUtil.GetStringOption(instanceIDOption, koinosUtil.GenerateBase58ID(5), *instanceID, yamlConfig.KoinosBridge, yamlConfig.Global)
	*reset = koinosUtil.GetBoolOption(resetOption, resetDefault, *reset, yamlConfig.KoinosBridge, yamlConfig.Global)

	*ethRPC = koinosUtil.GetStringOption(ethRPCOption, ethRPCDefault, *ethRPC, yamlConfig.KoinosBridge, yamlConfig.Global)
	*ethContract = koinosUtil.GetStringOption(ethContractOption, "", *ethContract, yamlConfig.KoinosBridge, yamlConfig.Global)
	*ethBlockStart = koinosUtil.GetStringOption(ethBlockStartOption, "", *ethBlockStart, yamlConfig.KoinosBridge, yamlConfig.Global)
	*ethPK = koinosUtil.GetStringOption(ethPKOption, "", *ethPK, yamlConfig.KoinosBridge, yamlConfig.Global)

	*koinosContract = koinosUtil.GetStringOption(koinosContractOption, "", *koinosContract, yamlConfig.KoinosBridge, yamlConfig.Global)
	*koinosBlockStart = koinosUtil.GetStringOption(koinosBlockStartOption, "", *koinosBlockStart, yamlConfig.KoinosBridge, yamlConfig.Global)
	*koinosPK = koinosUtil.GetStringOption(koinosPKOption, "", *koinosPK, yamlConfig.KoinosBridge, yamlConfig.Global)

	*validators = koinosUtil.GetStringSliceOption(validatorsOption, *validators, yamlConfig.KoinosBridge, yamlConfig.Global)
	*tokenAddressesArr = koinosUtil.GetStringSliceOption(tokensAddressesOption, *tokenAddressesArr, yamlConfig.KoinosBridge, yamlConfig.Global)

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(koinosUtil.GetAppDir(*baseDir, appName), logDir, appName+".log")
	err = log.InitLogger(*logLevel, false, logFilename, appID)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel))
	}

	// meta store
	metadataDbDir := path.Join(koinosUtil.GetAppDir((*baseDir), appName), "metadata")
	koinosUtil.EnsureDir(metadataDbDir)
	log.Infof("Opening database at %s", metadataDbDir)

	var metadataDbOpts = badger.DefaultOptions(metadataDbDir)
	metadataDbOpts.Logger = store.KoinosBadgerLogger{}
	var metadataDbBackend = store.NewBadgerBackend(metadataDbOpts)
	defer metadataDbBackend.Close()

	metadataStore := store.NewMetadataStore(metadataDbBackend)

	// koinos transactions store
	koinosDbDir := path.Join(koinosUtil.GetAppDir((*baseDir), appName), "koinos_transactions")
	koinosUtil.EnsureDir(koinosDbDir)
	log.Infof("Opening database at %s", koinosDbDir)

	var koinosDbOpts = badger.DefaultOptions(koinosDbDir)
	koinosDbOpts.Logger = store.KoinosBadgerLogger{}
	var koinosDbBackend = store.NewBadgerBackend(koinosDbOpts)
	defer koinosDbBackend.Close()

	koinosTxStore := store.NewKoinosTransactionsStore(koinosDbBackend)

	// ethereum transactions store
	ethDbDir := path.Join(koinosUtil.GetAppDir((*baseDir), appName), "ethereum_transactions")
	koinosUtil.EnsureDir(ethDbDir)
	log.Infof("Opening database at %s", ethDbDir)

	var ethDbOpts = badger.DefaultOptions(ethDbDir)
	ethDbOpts.Logger = store.KoinosBadgerLogger{}
	var ethDbBackend = store.NewBadgerBackend(ethDbOpts)
	defer ethDbBackend.Close()

	ethTxStore := store.NewEthTransactionsStore(ethDbBackend)

	// Reset backend if requested
	if *reset {
		log.Info("Resetting database")
		err := metadataDbBackend.Reset()
		if err != nil {
			panic(fmt.Sprintf("Error resetting metadata database: %s\n", err.Error()))
		}

		ethDbBackend.Reset()
		if err != nil {
			panic(fmt.Sprintf("Error resetting ethereum transactions database: %s\n", err.Error()))
		}

		koinosDbBackend.Reset()
		if err != nil {
			panic(fmt.Sprintf("Error resetting koinos transactions database: %s\n", err.Error()))
		}
	}

	// get metadata
	metadata, err := metadataStore.Get()

	if err != nil {
		panic(err)
	}

	if metadata == nil {
		metadata = &bridge_pb.Metadata{
			LastEthereumBlockParsed: *ethBlockStart,
		}
		metadataStore.Put(metadata)
	}

	log.Infof("LastEthereumBlockParsed %s", metadata.LastEthereumBlockParsed)

	// AMQP
	client := koinosmq.NewClient(*amqp, koinosmq.ExponentialBackoff)
	requestHandler := koinosmq.NewRequestHandler(*amqp)

	client.Start()

	log.Infof("starting listening amqp server %s\n", *amqp)

	requestHandler.SetRPCHandler(pluginName, handleRPC)

	requestHandler.Start()

	log.Info("request handler started")

	log.Info("Attempting to connect to p2p...")
	for {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		val, _ := IsConnectedToP2P(ctx, client)
		if val {
			log.Info("Connected to P2P")
			break
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	logsChan := make(chan types.Log, 10)

	startBlock, err := strconv.ParseUint(metadata.LastEthereumBlockParsed, 0, 64)

	if err != nil {
		panic(err)
	}

	startBlock++

	if *ethRPC != "none" {
		go fetchLogs(ctx, logsChan, startBlock, *ethRPC)
	}

	wg.Add(1)
	go processLogs(wg, logsChan, metadataStore, client)

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("closing")
	cancel()
	wg.Wait()
	log.Info("graceful stop completed")
}

func handleRPC(rpcType string, data []byte) ([]byte, error) {
	req := &rpcplugin.PluginRequest{}
	resp := &rpcplugin.PluginResponse{}

	err := proto.Unmarshal(data, req)
	if err != nil {
		log.Warnf("Received malformed request: 0x%v", hex.EncodeToString(data))
		eResp := prpc.ErrorResponse{Message: err.Error()}
		rErr := rpcplugin.PluginResponse_Error{Error: &eResp}
		resp.Response = &rErr
	} else {
		log.Infof("Received RPC request: 0x%v", hex.EncodeToString(data))
	}

	var outputBytes []byte
	outputBytes, err = proto.Marshal(resp)

	return outputBytes, err
}

// IsConnectedToBlockStore returns if the AMQP connection can currently communicate
// with the block store microservice.
func IsConnectedToP2P(ctx context.Context, client *koinosmq.Client) (bool, error) {
	args := &p2p.P2PRequest{
		Request: &p2p.P2PRequest_Reserved{
			Reserved: &rpc.ReservedRpc{},
		},
	}

	data, err := proto.Marshal(args)
	if err != nil {
		return false, fmt.Errorf("IsConnectedToP2P, %s", err)
	}

	var responseBytes []byte
	responseBytes, err = client.RPCContext(ctx, "application/octet-stream", "p2p", data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("IsConnectedToP2P, %s", err)
		}
		return false, fmt.Errorf("IsConnectedToP2P, %s", err)
	}

	responseVariant := &block_store.BlockStoreResponse{}
	err = proto.Unmarshal(responseBytes, responseVariant)
	if err != nil {
		return false, fmt.Errorf("IsConnectedToP2P, %s", err)
	}

	return true, nil
}

func testLoop(ctx context.Context, client *koinosmq.Client) {
	for {
		select {
		case <-time.After(time.Second * 5):
			fmt.Println("going to broadcast")
			pluginBroadcast := &broadcast.PluginBroadcast{}
			data, err := proto.Marshal(pluginBroadcast)
			if err != nil {
				fmt.Printf("could not serialize %s", err)
			}
			err = client.Broadcast("application/octet-stream", "plugin.bridge", data)
			if err != nil {
				fmt.Printf("could not broadcast %s", err)
			}
			fmt.Println("broadcasted")
		case <-ctx.Done():
			return
		}
	}
}

func fetchLogs(ctx context.Context, ch chan<- types.Log, startBlock uint64, ethRPC string) {
	ethCl, err := ethclient.Dial(ethRPC)

	if err != nil {
		panic(err)
	}

	defer ethCl.Close()

	fmt.Println("connected to Ethereum RPC")

	contractAddress := common.HexToAddress("0x5FbDB2315678afecb367f032d93F642f64180aa3")
	topic := common.HexToHash("0xb656d86127d7832fca2f3d9b253a58f55cec150b88b33cf54c8c514ab7ea623e")

	fromBlock := int64(startBlock)
	toBlock := fromBlock + 1

	for {
		select {
		case <-ctx.Done():
			log.Infof("stop fetching logs")
			close(ch)
			return

		case <-time.After(time.Second * 1):
			latestblock, err := ethCl.BlockNumber(ctx)

			if err != nil {
				panic(err)
			}

			log.Infof("latestblock: %d", latestblock)

			if uint64(toBlock) <= latestblock {
				query := ethereum.FilterQuery{
					FromBlock: big.NewInt(fromBlock),
					ToBlock:   big.NewInt(toBlock),
					// FromBlock: big.NewInt(0),
					// ToBlock:   big.NewInt(100),
					Addresses: []common.Address{
						contractAddress,
					},
					Topics: [][]common.Hash{
						{topic},
					},
				}
				log.Infof("fetchLogs: %d - %d", fromBlock, toBlock)

				logs, err := ethCl.FilterLogs(ctx, query)
				if err != nil {
					panic(err)
				}

				for _, vLog := range logs {

					// fmt.Println(vLog.BlockNumber)           // foo
					// fmt.Println(string(vLog.Address.Hex())) // bar
					// fmt.Println(vLog.Index)                 // bar
					// fmt.Println(vLog.TxHash)                // bar
					// fmt.Println(vLog.Topics)                // bar
					ch <- vLog
				}

				fromBlock++
				toBlock++
			} else {
				log.Info("waiting for new block")
			}
		}
	}
}

func processLogs(wg *sync.WaitGroup, ch <-chan types.Log, metaStore *store.MetadataStore, mqClient *koinosmq.Client) {
	defer wg.Done()
	var lastEthereumBlockParsed uint64

	for {
		select {
		case vLog, ok := <-ch:
			log.Info("receive for new logs")
			if ok {
				log.Info(fmt.Sprint(vLog.BlockNumber)) // foo
				log.Info(string(vLog.Address.Hex()))   // bar
				log.Info(fmt.Sprint(vLog.Index))       // bar
				log.Info(vLog.TxHash.Hex())            // bar
				// log.Info(vLog.Topics)                // bar
				lastEthereumBlockParsed = vLog.BlockNumber
				pluginBroadcast := &broadcast.PluginBroadcast{
					Data: []byte(fmt.Sprint(time.Now().UnixNano())),
				}
				bytes, err := proto.Marshal(pluginBroadcast)

				if err != nil {
					panic(err)
				}

				mqClient.Broadcast("application/octet-stream", "plugin.bridge", bytes)
			} else {
				log.Info("stop processing new logs")
				metadata, err := metaStore.Get()
				if err != nil {
					panic(err)
				}

				metadata.LastEthereumBlockParsed = strconv.FormatUint(lastEthereumBlockParsed, 10)

				metaStore.Put(metadata)
				return
			}
		}
	}
}
