package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"

	"github.com/dgraph-io/badger/v3"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/api"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/store"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/streamer"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/util"
	"github.com/mr-tron/base58"

	log "github.com/koinos/koinos-log-golang"
	koinosUtil "github.com/koinos/koinos-util-golang"

	flag "github.com/spf13/pflag"
)

const (
	basedirOption = "basedir"
)

const (
	basedirDefault    = "~/.koinos"
	instanceIDDefault = ""
	logLevelDefault   = "info"
	resetDefault      = false

	ethRPCDefault               = "http://127.0.0.1:8545/"
	ethBlockStartDefault        = 0
	ethMaxBlocksToStreamDefault = 500
	ethConfirmationsDefault     = 25
	ethPollingTimeDefault       = 3000

	koinosRPCDefault               = "http://127.0.0.1:8080/"
	koinosBlockStartDefault        = 0
	koinosMaxBlocksToStreamDefault = 500
	koinosPollingTimeDefault       = 3000

	emptyDefault = ""

	signaturesExpirationDefault uint = 60 * 60 * 1000 // 60mins
	apiUrlDefault                    = ":3000"
)

const (
	appName = "bridge"
	logDir  = "logs"
)

func main() {
	baseDir := flag.StringP(basedirOption, "d", basedirDefault, "the base directory")

	flag.Parse()

	var err error
	*baseDir, err = koinosUtil.InitBaseDir(*baseDir)
	if err != nil {
		panic(fmt.Sprintf("Could not initialize baseDir: %s", *baseDir))
	}

	yamlConfig := util.InitYamlConfig(*baseDir)

	fmt.Println(yamlConfig.Bridge.Reset)

	logLevel := util.GetStringOption(yamlConfig.Bridge.LogLevel, logLevelDefault)
	instanceID := util.GetStringOption(yamlConfig.Bridge.InstanceID, koinosUtil.GenerateBase58ID(5))
	reset := util.GetBoolOption(yamlConfig.Bridge.Reset, resetDefault)
	signaturesExpiration := util.GetUIntOption(yamlConfig.Bridge.SignaturesExpiration, signaturesExpirationDefault)
	apiUrl := util.GetStringOption(yamlConfig.Bridge.ApiUrl, apiUrlDefault)

	ethRPC := util.GetStringOption(yamlConfig.Bridge.EthereumRpc, ethRPCDefault)
	ethContract := util.GetStringOption(yamlConfig.Bridge.EthereumContract, emptyDefault)
	ethBlockStart := util.GetUInt64Option(yamlConfig.Bridge.EthereumBlockStart, ethBlockStartDefault)
	ethMaxBlocksToStream := util.GetUInt64Option(yamlConfig.Bridge.EthereumMaxBlocksStream, ethMaxBlocksToStreamDefault)
	ethConfirmations := util.GetUInt64Option(yamlConfig.Bridge.EthereumConfirmations, ethConfirmationsDefault)
	ethPK := util.GetStringOption(yamlConfig.Bridge.EthereumPK, emptyDefault)
	ethPollingTime := util.GetUIntOption(yamlConfig.Bridge.EthereumPollingTime, ethPollingTimeDefault)

	koinosRPC := util.GetStringOption(yamlConfig.Bridge.KoinosRpc, koinosRPCDefault)
	koinosContract := util.GetStringOption(yamlConfig.Bridge.KoinosContract, emptyDefault)
	koinosBlockStart := util.GetUInt64Option(yamlConfig.Bridge.KoinosBlockStart, koinosBlockStartDefault)
	koinosMaxBlocksToStream := util.GetUInt64Option(yamlConfig.Bridge.KoinosMaxBlocksStream, koinosMaxBlocksToStreamDefault)
	koinosPK := util.GetStringOption(yamlConfig.Bridge.KoinosPK, emptyDefault)
	koinosPollingTime := util.GetUIntOption(yamlConfig.Bridge.KoinosPollingTime, koinosPollingTimeDefault)

	validators := make(map[string]util.ValidatorConfig)
	tokenAddresses := make(map[string]util.TokenConfig)

	for _, validator := range yamlConfig.Bridge.Validators {
		validators[validator.KoinosAddress] = validator
		validators[validator.EthereumAddress] = validator
	}

	for _, tokenAddr := range yamlConfig.Bridge.Tokens {
		tokenAddresses[tokenAddr.KoinosAddress] = tokenAddr
		tokenAddresses[tokenAddr.EthereumAddress] = tokenAddr
	}

	appID := fmt.Sprintf("%s.%s", appName, instanceID)

	// Initialize logger
	logFilename := path.Join(koinosUtil.GetAppDir(*baseDir, appName), logDir, appName+".log")
	err = log.InitLogger(logLevel, false, logFilename, appID)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", logLevel))
	}

	// keys management
	koinosPKbytes, err := koinosUtil.DecodeWIF(koinosPK)
	if err != nil {
		log.Error(err.Error())
		panic(err)
	}

	koinosKey, err := koinosUtil.NewKoinosKeysFromBytes(koinosPKbytes)
	koinosAddress := base58.Encode(koinosKey.AddressBytes())

	if err != nil {
		log.Error(err.Error())
		panic(err)
	}
	log.Infof("Node koinosAddress %s", koinosAddress)

	ethPrivateKey, err := crypto.HexToECDSA(ethPK)
	if err != nil {
		log.Error(err.Error())
		panic(err)
	}
	ethAddress := crypto.PubkeyToAddress(ethPrivateKey.PublicKey).Hex()
	log.Infof("Node ethAddress %s", ethAddress)

	// metadata store
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

	koinosTxStore := store.NewTransactionsStore(koinosDbBackend)

	// ethereum transactions store
	ethDbDir := path.Join(koinosUtil.GetAppDir((*baseDir), appName), "ethereum_transactions")
	koinosUtil.EnsureDir(ethDbDir)
	log.Infof("Opening database at %s", ethDbDir)

	var ethDbOpts = badger.DefaultOptions(ethDbDir)
	ethDbOpts.Logger = store.KoinosBadgerLogger{}
	var ethDbBackend = store.NewBadgerBackend(ethDbOpts)
	defer ethDbBackend.Close()

	ethTxStore := store.NewTransactionsStore(ethDbBackend)

	// Reset backend if requested
	if reset {
		log.Info("Resetting database")
		err := metadataDbBackend.Reset()
		if err != nil {
			log.Error(err.Error())
			panic(fmt.Sprintf("Error resetting metadata database: %s\n", err.Error()))
		}

		ethDbBackend.Reset()
		if err != nil {
			log.Error(err.Error())
			panic(fmt.Sprintf("Error resetting ethereum transactions database: %s\n", err.Error()))
		}

		koinosDbBackend.Reset()
		if err != nil {
			log.Error(err.Error())
			panic(fmt.Sprintf("Error resetting koinos transactions database: %s\n", err.Error()))
		}
	}

	// get metadata
	metadata, err := metadataStore.Get()

	if err != nil {
		log.Error(err.Error())
		panic(err)
	}

	if ethBlockStart > 0 {
		metadata.LastEthereumBlockParsed = ethBlockStart - 1
	}

	if koinosBlockStart > 0 {
		metadata.LastKoinosBlockParsed = koinosBlockStart - 1
	}

	log.Infof("LastEthereumBlockParsed: %d", metadata.LastEthereumBlockParsed)
	log.Infof("LastKoinosBlockParsed: %d", metadata.LastKoinosBlockParsed)

	// blockchains streaming
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	if ethMaxBlocksToStream > 0 {
		wg.Add(1)
		go streamer.StreamEthereumBlocks(
			&wg,
			ctx,
			metadataStore,
			metadata.LastEthereumBlockParsed,
			ethRPC,
			ethContract,
			ethMaxBlocksToStream,
			koinosPKbytes,
			koinosAddress,
			koinosContract,
			tokenAddresses,
			ethTxStore,
			koinosTxStore,
			signaturesExpiration,
			validators,
			ethConfirmations,
			ethPollingTime,
		)
	}

	if koinosMaxBlocksToStream > 0 {
		wg.Add(1)
		go streamer.StreamKoinosBlocks(
			&wg,
			ctx,
			metadataStore,
			metadata.LastKoinosBlockParsed,
			koinosRPC,
			ethPrivateKey,
			ethAddress,
			ethContract,
			koinosMaxBlocksToStream,
			koinosPKbytes,
			koinosAddress,
			koinosContract,
			tokenAddresses,
			ethTxStore,
			koinosTxStore,
			signaturesExpiration,
			validators,
			koinosPollingTime,
		)
	}

	// Run API server
	go func() {
		api := api.NewApi(ethTxStore, koinosTxStore, koinosContract, ethContract, validators, koinosAddress, ethAddress)
		mux := http.NewServeMux()
		mux.HandleFunc("/GetEthereumTransaction", api.GetEthereumTransaction)
		mux.HandleFunc("/GetKoinosTransaction", api.GetKoinosTransaction)
		mux.HandleFunc("/SubmitSignature", api.SubmitSignature)

		httpServer := &http.Server{
			Addr:        apiUrl,
			Handler:     mux,
			BaseContext: func(_ net.Listener) context.Context { return ctx },
		}
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Errorf("HTTP server ListenAndServe: %v", err)
		}
	}()

	wg.Wait()
	log.Info("graceful stop completed")
}
