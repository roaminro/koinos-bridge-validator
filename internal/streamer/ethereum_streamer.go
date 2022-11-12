package streamer

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/koinos/koinos-log-golang"

	"github.com/mr-tron/base58"

	"github.com/koinos-bridge/koinos-bridge-validator/internal/store"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/util"
	"github.com/koinos-bridge/koinos-bridge-validator/proto/build/github.com/koinos-bridge/koinos-bridge-validator/bridge_pb"

	"google.golang.org/protobuf/proto"
)

func StreamEthereumBlocks(
	ctx context.Context,
	metadataStore *store.MetadataStore,
	savedLastEthereumBlockParsed string,
	ethRPC string,
	ethContractStr string,
	ethMaxBlocksToStreamStr string,
	koinosPK []byte,
	koinosAddress string,
	koinosContractStr string,
	tokenAddresses map[string]util.TokenConfig,
	ethTxStore *store.TransactionsStore,
	koinosTxStore *store.TransactionsStore,
	signaturesExpiration uint,
	validators map[string]util.ValidatorConfig,
	ethConfirmationsStr string,
) {
	tokensLockedEventTopic := crypto.Keccak256Hash([]byte("TokensLockedEvent(address,address,uint256,string,uint256)"))
	tokensLockedEventAbiStr := `[{
		"anonymous": false,
		"inputs": [
		  {
			"indexed": false,
			"internalType": "address",
			"name": "from",
			"type": "address"
		  },
		  {
			"indexed": false,
			"internalType": "address",
			"name": "token",
			"type": "address"
		  },
		  {
			"indexed": false,
			"internalType": "uint256",
			"name": "amount",
			"type": "uint256"
		  },
		  {
			"indexed": false,
			"internalType": "string",
			"name": "recipient",
			"type": "string"
		  },
		  {
			"indexed": false,
			"internalType": "uint256",
			"name": "blocktime",
			"type": "uint256"
		  }
		],
		"name": "TokensLockedEvent",
		"type": "event"
	  }]`

	tokensLockedEventAbi, err := abi.JSON(strings.NewReader(tokensLockedEventAbiStr))

	if err != nil {
		panic(err)
	}

	transferCompletedEventTopic := crypto.Keccak256Hash([]byte("TransferCompletedEvent(bytes,uint256)"))
	transferCompletedEventAbiStr := `[{
		"anonymous": false,
		"inputs": [
		  {
			"indexed": false,
			"internalType": "bytes",
			"name": "txId",
			"type": "bytes"
		  },
		  {
			"indexed": false,
			"internalType": "uint256",
			"name": "operationId",
			"type": "uint256"
		  }
		],
		"name": "TransferCompletedEvent",
		"type": "event"
	  }]`

	transferCompletedEventAbi, err := abi.JSON(strings.NewReader(transferCompletedEventAbiStr))

	if err != nil {
		panic(err)
	}

	requestNewSignaturesEventTopic := crypto.Keccak256Hash([]byte("RequestNewSignaturesEvent(bytes,uint256)"))
	requestNewSignaturesEventAbiStr := `[{
		"anonymous": false,
		"inputs": [
		  {
			"indexed": false,
			"internalType": "bytes",
			"name": "txId",
			"type": "bytes"
		  },
		  {
			"indexed": false,
			"internalType": "uint256",
			"name": "blocktime",
			"type": "uint256"
		  }
		],
		"name": "RequestNewSignaturesEvent",
		"type": "event"
	  }]`

	requestNewSignaturesEventAbi, err := abi.JSON(strings.NewReader(requestNewSignaturesEventAbiStr))

	if err != nil {
		panic(err)
	}

	ethCl, err := ethclient.Dial(ethRPC)

	if err != nil {
		panic(err)
	}

	defer ethCl.Close()

	fmt.Println("connected to Ethereum RPC")

	ethMaxBlocksToStream, err := strconv.ParseUint(ethMaxBlocksToStreamStr, 0, 64)
	if err != nil {
		panic(err)
	}

	startBlock, err := strconv.ParseUint(savedLastEthereumBlockParsed, 0, 64)
	if err != nil {
		panic(err)
	}

	ethConfirmations, err := strconv.ParseUint(ethConfirmationsStr, 0, 64)
	if err != nil {
		panic(err)
	}

	startBlock++

	ethContractAddr := common.HexToAddress(ethContractStr)
	koinosContractAddr, err := base58.Decode(koinosContractStr)
	if err != nil {
		panic(err)
	}

	var lastEthereumBlockParsed uint64
	fromBlock := startBlock

	for {
		select {
		case <-ctx.Done():
			log.Infof("stop streaming logs: %d", lastEthereumBlockParsed)
			metadataStore.Lock()
			defer metadataStore.Unlock()

			metadata, err := metadataStore.Get()
			if err != nil {
				panic(err)
			}

			metadata.LastEthereumBlockParsed = strconv.FormatUint(lastEthereumBlockParsed, 10)

			metadataStore.Put(metadata)
			return

		case <-time.After(time.Millisecond * 1000):
			latestblock, err := ethCl.BlockNumber(ctx)
			// trail by ethConfirmations blocks
			latestblock = latestblock - ethConfirmations

			if err != nil {
				panic(err)
			}

			log.Infof("latestblock: %d", latestblock)

			var blockDelta uint64 = 0

			if latestblock > fromBlock {
				blockDelta = latestblock - fromBlock
			}

			var toBlock = fromBlock + blockDelta

			if blockDelta > ethMaxBlocksToStream {
				toBlock = fromBlock + ethMaxBlocksToStream
			}

			if toBlock <= latestblock {
				query := ethereum.FilterQuery{
					FromBlock: big.NewInt(int64(fromBlock)),
					ToBlock:   big.NewInt(int64(toBlock)),
					Addresses: []common.Address{
						ethContractAddr,
					},
					Topics: [][]common.Hash{
						{
							tokensLockedEventTopic,
							transferCompletedEventTopic,
							requestNewSignaturesEventTopic,
						},
					},
				}
				log.Infof("fetched eth logs: %d - %d", fromBlock, toBlock)

				logs, err := ethCl.FilterLogs(ctx, query)
				if err != nil {
					panic(err)
				}

				for _, vLog := range logs {
					// do not processed removed logs
					if vLog.Removed {
						continue
					}

					if vLog.Topics[0] == tokensLockedEventTopic {
						// if TokensLockedEvent
						processEthereumTokensLockedEvent(
							koinosPK,
							koinosAddress,
							koinosContractAddr,
							tokenAddresses,
							ethTxStore,
							signaturesExpiration,
							validators,
							vLog,
							tokensLockedEventAbi,
						)
					} else if vLog.Topics[0] == transferCompletedEventTopic {
						// if TransferCompletedEvenet
						processEthereumTransferCompletedEvent(
							koinosTxStore,
							vLog,
							transferCompletedEventAbi,
						)
					} else if vLog.Topics[0] == requestNewSignaturesEventTopic {
						// if RequestNewSignaturesEvent
						processEthereumRequestNewSignaturesEvent(
							koinosPK,
							koinosAddress,
							koinosContractAddr,
							tokenAddresses,
							ethTxStore,
							signaturesExpiration,
							validators,
							vLog,
							requestNewSignaturesEventAbi,
						)
					}

					lastEthereumBlockParsed = vLog.BlockNumber
				}

				if len(logs) == 0 {
					// if no logs available
					fromBlock = toBlock + 1
				} else {
					fromBlock = lastEthereumBlockParsed + 1
				}
			} else {
				log.Info("waiting for new block: " + fmt.Sprint(fromBlock))
			}
		}
	}
}

func processEthereumRequestNewSignaturesEvent(
	koinosPK []byte,
	koinosAddress string,
	koinosContractAddr []byte,
	tokenAddresses map[string]util.TokenConfig,
	ethTxStore *store.TransactionsStore,
	signaturesExpiration uint,
	validators map[string]util.ValidatorConfig,
	vLog types.Log,
	eventAbi abi.ABI,
) {
	// parse event
	event := struct {
		TxId      []byte
		Blocktime *big.Int
	}{}

	err := eventAbi.UnpackIntoInterface(&event, "RequestNewSignaturesEvent", vLog.Data)
	if err != nil {
		panic(err)
	}

	blockNumber := fmt.Sprint(vLog.BlockNumber)
	requestTxId := vLog.TxHash.Hex()
	transactionId := "0x" + common.Bytes2Hex(event.TxId)
	blocktime := event.Blocktime.Uint64()
	newExpiration := blocktime + uint64(signaturesExpiration)

	log.Infof("new Eth RequestNewSignaturesEvent | request block: %s | request tx: %s | tx: %s", blockNumber, requestTxId, transactionId)

	ethTxStore.Lock()
	ethTx, err := ethTxStore.Get(transactionId)
	if err != nil {
		panic(err)
	}

	if ethTx != nil && ethTx.Status != bridge_pb.TransactionStatus_completed {
		// can only request signatures after 2x expiration time
		allowedRequestNewSignaturesBlockTime := ethTx.Expiration + uint64(signaturesExpiration)

		if blocktime >= allowedRequestNewSignaturesBlockTime {
			txId := common.FromHex(ethTx.Id)
			koinosToken, err := base58.Decode(ethTx.KoinosToken)
			if err != nil {
				panic(err)
			}

			recipient, err := base58.Decode(ethTx.Recipient)
			if err != nil {
				panic(err)
			}

			amount, err := strconv.ParseUint(ethTx.Amount, 0, 64)
			if err != nil {
				panic(err)
			}

			completeTransferHash := &bridge_pb.CompleteTransferHash{
				Action:        bridge_pb.ActionId_complete_transfer,
				TransactionId: txId,
				Token:         koinosToken,
				Recipient:     recipient,
				Amount:        amount,
				ContractId:    koinosContractAddr,
				Expiration:    newExpiration,
			}

			completeTransferHashBytes, err := proto.Marshal(completeTransferHash)
			if err != nil {
				panic(err)
			}

			hash := sha256.Sum256(completeTransferHashBytes)
			hashB64 := base64.URLEncoding.EncodeToString(hash[:])

			sigBytes := util.SignKoinosHash(koinosPK, hash[:])
			sigB64 := base64.URLEncoding.EncodeToString(sigBytes)

			// cleanup signatures
			newSignatures := make(map[string]string)
			newSignatures[koinosAddress] = sigB64

			for index, validatr := range ethTx.Validators {
				_, found := newSignatures[validatr]
				if !found {
					// only keep signatures that match the new hash
					sig := ethTx.Signatures[index]
					recoveredAddr, _ := util.RecoverKoinosAddressFromSignature(sig, hash[:])

					if recoveredAddr == validatr {
						newSignatures[validatr] = sig
					}
				}
			}

			// update tx
			ethTx.Expiration = newExpiration
			ethTx.Hash = hashB64
			ethTx.Validators = []string{}
			ethTx.Signatures = []string{}
			for val, sig := range newSignatures {
				ethTx.Validators = append(ethTx.Validators, val)
				ethTx.Signatures = append(ethTx.Signatures, sig)
			}

			ethTx.Status = bridge_pb.TransactionStatus_gathering_signatures

			if len(ethTx.Signatures) >= ((((len(validators)/2)*10)/3)*2)/10+1 {
				ethTx.Status = bridge_pb.TransactionStatus_signed
			}

			err = ethTxStore.Put(transactionId, ethTx)

			if err != nil {
				panic(err)
			}

			ethTxStore.Unlock()

			// broadcast transaction
			koinosSignatures, _ := util.BroadcastTransaction(ethTx, koinosPK, koinosAddress, validators)

			// update the transaction with signatures we may have gotten back from the broadcast
			ethTxStore.Lock()

			ethTx, err = ethTxStore.Get(transactionId)
			if err != nil {
				panic(err)
			}

			for index, validatr := range ethTx.Validators {
				_, found := koinosSignatures[validatr]
				if !found {
					koinosSignatures[validatr] = ethTx.Signatures[index]
				}
			}

			ethTx.Validators = []string{}
			ethTx.Signatures = []string{}
			for val, sig := range koinosSignatures {
				ethTx.Validators = append(ethTx.Validators, val)
				ethTx.Signatures = append(ethTx.Signatures, sig)
			}

			if ethTx.Status != bridge_pb.TransactionStatus_completed &&
				len(ethTx.Signatures) >= ((((len(validators)/2)*10)/3)*2)/10+1 {
				ethTx.Status = bridge_pb.TransactionStatus_signed
			}

			err = ethTxStore.Put(transactionId, ethTx)

			if err != nil {
				panic(err)
			}

			ethTxStore.Unlock()
		} else {
			log.Infof("Cannot request new signatures for Eth tx %s yet (current blocktime %d vs allowed blocktime %d)", transactionId, blocktime, allowedRequestNewSignaturesBlockTime)
			ethTxStore.Unlock()
		}
	} else {
		log.Infof("Eth tx %s does not exist or is already completed", transactionId)
		ethTxStore.Unlock()
	}
}

func processEthereumTransferCompletedEvent(
	koinosTxStore *store.TransactionsStore,
	vLog types.Log,
	eventAbi abi.ABI,
) {
	// parse event
	event := struct {
		TxId        []byte
		OperationId *big.Int
	}{}

	err := eventAbi.UnpackIntoInterface(&event, "TransferCompletedEvent", vLog.Data)
	if err != nil {
		panic(err)
	}

	blockNumber := fmt.Sprint(vLog.BlockNumber)
	ethTxId := vLog.TxHash.Hex()
	koinosTxId := "0x" + common.Bytes2Hex(event.TxId)
	koinosOpId := event.OperationId.String()

	log.Infof("new Eth LogTransferCompleted event | block: %s | tx: %s | koinos tx: %s | koinos op: %s", blockNumber, ethTxId, koinosTxId, koinosOpId)

	txKey := koinosTxId + "-" + koinosOpId
	koinosTxStore.Lock()
	koinosTx, err := koinosTxStore.Get(txKey)
	if err != nil {
		panic(err)
	}

	if koinosTx == nil {
		log.Warnf("koinos transaction %s - op %s does not exist", koinosTxId, koinosOpId)
		koinosTx = &bridge_pb.Transaction{}
		koinosTx.Type = bridge_pb.TransactionType_koinos
	}

	koinosTx.Status = bridge_pb.TransactionStatus_completed
	koinosTx.CompletionTransactionId = ethTxId

	err = koinosTxStore.Put(txKey, koinosTx)
	if err != nil {
		panic(err)
	}
	koinosTxStore.Unlock()
}

func processEthereumTokensLockedEvent(
	koinosPK []byte,
	koinosAddress string,
	koinosContractAddr []byte,
	tokenAddresses map[string]util.TokenConfig,
	ethTxStore *store.TransactionsStore,
	signaturesExpiration uint,
	validators map[string]util.ValidatorConfig,
	vLog types.Log,
	eventAbi abi.ABI,
) {
	// parse event
	event := struct {
		Token     common.Address
		From      common.Address
		Recipient string
		Amount    *big.Int
		Blocktime *big.Int
	}{}

	err := eventAbi.UnpackIntoInterface(&event, "TokensLockedEvent", vLog.Data)
	if err != nil {
		panic(err)
	}

	blockNumber := fmt.Sprint(vLog.BlockNumber)
	txId := vLog.TxHash.Bytes()
	txIdHex := vLog.TxHash.Hex()
	ethFrom := event.From.Hex()
	ethToken := event.Token.Hex()
	amount := event.Amount.Uint64()
	blocktime := event.Blocktime.Uint64()

	koinosToken, err := base58.Decode(tokenAddresses[ethToken].KoinosAddress)
	if err != nil {
		panic(err)
	}

	recipient, err := base58.Decode(event.Recipient)
	if err != nil {
		panic(err)
	}

	log.Infof("new Eth TokensLockedEvent | block: %s | tx: %s | ETH token: %s | Koinos token: %s | From: %s | recipient: %s | amount: %s ", blockNumber, txIdHex, ethToken, tokenAddresses[ethToken].KoinosAddress, ethFrom, event.Recipient, event.Amount.String())

	expiration := blocktime + uint64(signaturesExpiration)

	// sign the transaction
	completeTransferHash := &bridge_pb.CompleteTransferHash{
		Action:        bridge_pb.ActionId_complete_transfer,
		TransactionId: txId,
		Token:         koinosToken,
		Recipient:     recipient,
		Amount:        amount,
		ContractId:    koinosContractAddr,
		Expiration:    expiration,
	}

	completeTransferHashBytes, err := proto.Marshal(completeTransferHash)
	if err != nil {
		panic(err)
	}

	hash := sha256.Sum256(completeTransferHashBytes)
	hashB64 := base64.URLEncoding.EncodeToString(hash[:])

	sigBytes := util.SignKoinosHash(koinosPK, hash[:])
	sigB64 := base64.URLEncoding.EncodeToString(sigBytes)

	// store the transaction
	ethTxStore.Lock()

	ethTx, err := ethTxStore.Get(txIdHex)
	if err != nil {
		panic(err)
	}

	if ethTx == nil {
		ethTx = &bridge_pb.Transaction{}
		ethTx.Validators = []string{koinosAddress}
		ethTx.Signatures = []string{sigB64}
	} else {
		if ethTx.Hash != "" && ethTx.Hash != hashB64 {
			errMsg := fmt.Sprintf("the calculated hash for tx %s is different than the one already received %s != calculated %s", txIdHex, ethTx.Hash, hashB64)
			log.Errorf(errMsg)
			panic(fmt.Errorf(errMsg))
		}
		ethTx.Validators = append(ethTx.Validators, koinosAddress)
		ethTx.Signatures = append(ethTx.Signatures, sigB64)
	}

	ethTx.Type = bridge_pb.TransactionType_ethereum
	ethTx.Id = txIdHex
	ethTx.From = ethFrom
	ethTx.EthToken = ethToken
	ethTx.KoinosToken = tokenAddresses[ethToken].KoinosAddress
	ethTx.Amount = event.Amount.String()
	ethTx.Recipient = event.Recipient
	ethTx.Hash = hashB64
	ethTx.BlockNumber = vLog.BlockNumber
	ethTx.BlockTime = blocktime
	ethTx.Expiration = expiration
	if ethTx.Status != bridge_pb.TransactionStatus_completed {
		ethTx.Status = bridge_pb.TransactionStatus_gathering_signatures
	}

	err = ethTxStore.Put(txIdHex, ethTx)

	if err != nil {
		panic(err)
	}

	ethTxStore.Unlock()

	// broadcast transaction
	signatures, _ := util.BroadcastTransaction(ethTx, koinosPK, koinosAddress, validators)

	// update the transaction with signatures we may have gotten back from the broadcast
	ethTxStore.Lock()

	ethTx, err = ethTxStore.Get(txIdHex)
	if err != nil {
		panic(err)
	}

	for index, validatr := range ethTx.Validators {
		_, found := signatures[validatr]
		if !found {
			signatures[validatr] = ethTx.Signatures[index]
		}
	}

	ethTx.Validators = []string{}
	ethTx.Signatures = []string{}
	for val, sig := range signatures {
		ethTx.Validators = append(ethTx.Validators, val)
		ethTx.Signatures = append(ethTx.Signatures, sig)
	}

	if ethTx.Status != bridge_pb.TransactionStatus_completed &&
		len(ethTx.Signatures) >= ((((len(validators)/2)*10)/3)*2)/10+1 {
		ethTx.Status = bridge_pb.TransactionStatus_signed
	}

	err = ethTxStore.Put(txIdHex, ethTx)

	if err != nil {
		panic(err)
	}

	ethTxStore.Unlock()
}
