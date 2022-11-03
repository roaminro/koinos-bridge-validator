package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/dgraph-io/badger/v3"
	"github.com/roaminroe/koinos-bridge-validator/internal/api"
	"github.com/roaminroe/koinos-bridge-validator/internal/ethereum"
	"github.com/roaminroe/koinos-bridge-validator/internal/store"
	"github.com/roaminroe/koinos-bridge-validator/internal/util"
	"github.com/roaminroe/koinos-bridge-validator/proto/build/github.com/roaminroe/koinos-bridge-validator/bridge_pb"

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
	ethBlockStartDefault        = "0"
	ethMaxBlocksToStreamDefault = "500"

	koinosRPCDefault               = "http://127.0.0.1:8080/"
	koinosBlockStartDefault        = "0"
	koinosMaxBlocksToStreamDefault = "500"

	emptyDefault = ""

	signaturesExpirationDefault uint = 60 * 30 // 30mins
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
	*baseDir = koinosUtil.InitBaseDir(*baseDir)
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
	ethBlockStart := util.GetStringOption(yamlConfig.Bridge.EthereumBlockStart, ethBlockStartDefault)
	ethMaxBlocksToStream := util.GetStringOption(yamlConfig.Bridge.EthereumMaxBlocksStream, ethMaxBlocksToStreamDefault)
	// ethPK := util.GetStringOption(yamlConfig.Bridge.EthereumPK, emptyDefault)

	// koinosRPC := util.GetStringOption(yamlConfig.Bridge.KoinosRpc, koinosRPCDefault)
	koinosContract := util.GetStringOption(yamlConfig.Bridge.KoinosContract, emptyDefault)
	// koinosBlockStart := util.GetStringOption(yamlConfig.Bridge.KoinosBlockStart, koinosBlockStartDefault)
	// koinosMaxBlocksToStream := util.GetStringOption(yamlConfig.Bridge.KoinosMaxBlocksStream, koinosMaxBlocksToStreamDefault)
	koinosPK := util.GetStringOption(yamlConfig.Bridge.KoinosPK, emptyDefault)

	validators := make(map[string]string)
	tokenAddresses := make(map[string]string)

	for _, validator := range yamlConfig.Bridge.Validators {
		validators[validator.KoinosAddress] = validator.EthereumAddress
		validators[validator.EthereumAddress] = validator.KoinosAddress
	}

	for _, tokenAddr := range yamlConfig.Bridge.Tokens {
		tokenAddresses[tokenAddr.KoinosAddress] = tokenAddr.EthereumAddress
		tokenAddresses[tokenAddr.EthereumAddress] = tokenAddr.KoinosAddress
	}

	appID := fmt.Sprintf("%s.%s", appName, instanceID)

	// Initialize logger
	logFilename := path.Join(koinosUtil.GetAppDir(*baseDir, appName), logDir, appName+".log")
	err = log.InitLogger(logLevel, false, logFilename, appID)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", logLevel))
	}

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
			LastEthereumBlockParsed: ethBlockStart,
		}
		metadataStore.Put(metadata)
	}

	log.Infof("LastEthereumBlockParsed %s", metadata.LastEthereumBlockParsed)

	// log.Info("request handler started")

	// blockchains streaming

	ctx, cancel := context.WithCancel(context.Background())

	if ethRPC != "none" {
		go ethereum.StreamEthereumBlocks(
			ctx,
			metadataStore,
			metadata.LastEthereumBlockParsed,
			ethRPC,
			ethContract,
			ethMaxBlocksToStream,
			koinosPK,
			koinosContract,
			tokenAddresses,
			ethTxStore,
			signaturesExpiration,
		)
	}

	// Run API server
	go func() {
		api := api.NewApi(ethTxStore, koinosTxStore, koinosContract, ethContract, validators)
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

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("closing service gracefully")
	cancel()
	log.Info("graceful stop completed")
}
