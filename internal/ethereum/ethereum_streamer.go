package ethereum

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/koinos/koinos-log-golang"

	"github.com/mr-tron/base58"

	"github.com/koinos-bridge/koinos-bridge-validator/internal/store"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/util"
	"github.com/koinos-bridge/koinos-bridge-validator/proto/build/github.com/koinos-bridge/koinos-bridge-validator/bridge_pb"

	"google.golang.org/protobuf/encoding/protojson"
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
	signaturesExpiration uint,
	validators map[string]util.ValidatorConfig,
) {
	// topic = keccak-256("LogTokensLocked(address,address,uint256,string,uint256)")
	topic := common.HexToHash("0xdd58de01fef6397c5b9ce2dc1f605fa2dd517bf630021e009a6ecadbc0abe23f")
	eventAbiStr := `[{
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
		"name": "LogTokensLocked",
		"type": "event"
	  }]`

	eventAbi, err := abi.JSON(strings.NewReader(eventAbiStr))

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
			log.Infof("stop streaming logs")
			metadataStore.Lock()
			defer metadataStore.Unlock()

			metadata, err := metadataStore.Get()
			if err != nil {
				panic(err)
			}

			log.Infof("stop streaming logs %d", metadata.LastEthereumBlockParsed)

			metadata.LastEthereumBlockParsed = strconv.FormatUint(lastEthereumBlockParsed, 10)

			metadataStore.Put(metadata)
			return

		case <-time.After(time.Millisecond * 1000):
			latestblock, err := ethCl.BlockNumber(ctx)

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
						{topic},
					},
				}
				log.Infof("fetched eth logs: %d - %d", fromBlock, toBlock)

				logs, err := ethCl.FilterLogs(ctx, query)
				if err != nil {
					panic(err)
				}

				for _, vLog := range logs {
					// parse event
					event := struct {
						Token     common.Address
						From      common.Address
						Recipient string
						Amount    *big.Int
						Blocktime *big.Int
					}{}

					err := eventAbi.UnpackIntoInterface(&event, "LogTokensLocked", vLog.Data)
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

					log.Infof("new Eth event | block: %s | tx: %s | ETH token: %s | Koinos token: %s | From: %s | recipient: %s | amount: %s ", blockNumber, txIdHex, ethToken, tokenAddresses[ethToken], ethFrom, event.Recipient, event.Amount.String())

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
					hashB64 := base64.StdEncoding.EncodeToString(hash[:])

					sigBytes := util.SignKoinosHash(koinosPK, hash[:])
					sigB64 := base64.StdEncoding.EncodeToString(sigBytes)

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
						if ethTx.Hash != hashB64 {
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
					ethTx.Status = bridge_pb.TransactionStatus_gathering_signatures

					err = ethTxStore.Put(txIdHex, ethTx)

					if err != nil {
						panic(err)
					}

					ethTxStore.Unlock()

					// broadcast transaction
					signatures, _ := broadcastTransaction(ethTx, koinosPK, koinosAddress, validators)

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

					if len(ethTx.Signatures) >= ((((len(validators)/2)*10)/3)*2)/10+1 {
						ethTx.Status = bridge_pb.TransactionStatus_signed
					}

					err = ethTxStore.Put(txIdHex, ethTx)

					if err != nil {
						panic(err)
					}

					ethTxStore.Unlock()

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

func broadcastTransaction(tx *bridge_pb.Transaction, koinosPK []byte, koinosAddress string, validators map[string]util.ValidatorConfig) (map[string]string, error) {
	signatures := make(map[string]string)

	txBytes, err := proto.Marshal(tx)
	if err != nil {
		return nil, err
	}

	hash := sha256.Sum256(txBytes)
	sigBytes := util.SignKoinosHash(koinosPK, hash[:])
	sigB64 := base64.StdEncoding.EncodeToString(sigBytes)

	submittedSignature := &bridge_pb.SubmittedSignature{
		Transaction: tx,
		Signature:   sigB64,
	}

	submittedSignatureBytes, err := protojson.Marshal(submittedSignature)
	if err != nil {
		return nil, err
	}

	processedApiUrls := make(map[string]bool)

	for _, validator := range validators {
		// don't send to yourself
		if validator.KoinosAddress == koinosAddress {
			continue
		}

		// since the map has the validators ethereum addresses and koinos addresses as key
		// make sure to not send twice to same node
		_, found := processedApiUrls[validator.ApiUrl]
		if found {
			continue
		}

		bodyReader := bytes.NewReader(submittedSignatureBytes)
		req, err := http.NewRequest(http.MethodPost, validator.ApiUrl+"/SubmitSignature", bodyReader)

		if err != nil {
			log.Errorf("client: could not create request: %s\n", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		client := http.Client{
			Timeout: 30 * time.Second,
		}

		res, err := client.Do(req)
		if err != nil {
			log.Errorf("client: error making http request to %s: %s\n", validator.KoinosAddress, err)
			continue
		}

		log.Infof("broadcast %s: status code %d for tx %s\n", validator.KoinosAddress, res.StatusCode, tx.Id)
		signatureBytes, _ := ioutil.ReadAll(res.Body)
		signature := string(signatureBytes)

		if signature != "" {
			log.Infof("client: received signature %s\n", signatureBytes)
			signatures[validator.KoinosAddress] = signature
		}

		processedApiUrls[validator.ApiUrl] = true
	}

	return signatures, nil
}
