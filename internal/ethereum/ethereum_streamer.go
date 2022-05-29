package ethereum

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
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	koinosUtil "github.com/koinos/koinos-util-golang"

	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/mr-tron/base58"

	"github.com/roaminroe/koinos-bridge-validator/internal/store"
	"github.com/roaminroe/koinos-bridge-validator/internal/util"
	"github.com/roaminroe/koinos-bridge-validator/proto/build/github.com/roaminroe/koinos-bridge-validator/bridge_pb"

	"google.golang.org/protobuf/proto"
)

func StreamEthereumBlocks(
	ctx context.Context,
	mqClient *koinosmq.Client,
	metadataStore *store.MetadataStore,
	savedLastEthereumBlockParsed string,
	ethRPC string,
	ethContractStr string,
	ethMaxBlocksToStreamStr string,
	noP2P bool,
	koinosPKStr string,
	koinosContractStr string,
	tokenAddresses map[string]string,
	ethTxStore *store.TransactionsStore,
) {
	koinosPK, err := koinosUtil.DecodeWIF(koinosPKStr)
	if err != nil {
		panic(err)
	}

	koinosKey, err := koinosUtil.NewKoinosKeysFromBytes(koinosPK)

	if err != nil {
		panic(err)
	}

	topic := common.HexToHash("0x4c21824fb18b7a1c065b8a879a8952f7513e394fdc44b597dd51ea954cf37bf9")
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
			metadata, err := metadataStore.Get()
			if err != nil {
				panic(err)
			}

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

					koinosToken, err := base58.Decode(tokenAddresses[ethToken])
					if err != nil {
						panic(err)
					}

					recipient, err := base58.Decode(event.Recipient)
					if err != nil {
						panic(err)
					}

					log.Infof("new Eth event | block: %s | tx: %s | ETH token: %s | Koinos token: %s | From: %s | recipient: %s | amount: %s ", blockNumber, txIdHex, ethToken, tokenAddresses[ethToken], ethFrom, event.Recipient, event.Amount.String())

					// sign the transaction
					completeTransferHash := &bridge_pb.CompleteTransferHash{
						Action:        bridge_pb.ActionId_complete_transfer,
						TransactionId: txId,
						Token:         koinosToken,
						Recipient:     recipient,
						Amount:        amount,
						ContractId:    koinosContractAddr,
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
					ethTx, err := ethTxStore.Get(txIdHex)
					if err != nil {
						panic(err)
					}

					if ethTx == nil {
						ethTx = &bridge_pb.Transaction{}
						ethTx.Validators = []string{koinosKey.Public()}
						ethTx.Signatures = []string{sigB64}
					} else {
						if ethTx.Hash != hashB64 {
							errMsg := fmt.Sprintf("the calculated hash for tx %s is different than the one already received %s != calculated %s", txIdHex, ethTx.Hash, hashB64)
							log.Errorf(errMsg)
							panic(fmt.Errorf(errMsg))
						}
						ethTx.Validators = append(ethTx.Validators, koinosKey.Public())
						ethTx.Signatures = append(ethTx.Signatures, sigB64)
					}

					ethTx.Type = bridge_pb.TransactionType_ethereum
					ethTx.Id = txIdHex
					ethTx.OpId = ""
					ethTx.From = ethFrom
					ethTx.EthToken = ethToken
					ethTx.KoinosToken = tokenAddresses[ethToken]
					ethTx.Amount = event.Amount.String()
					ethTx.Recipient = event.Recipient
					ethTx.Hash = hashB64

					err = ethTxStore.Put(txIdHex, ethTx)

					if err != nil {
						panic(err)
					}

					if !noP2P {
						ethTxBytes, err := proto.Marshal(ethTx)
						if err != nil {
							panic(err)
						}

						pluginBroadcast := &broadcast.PluginBroadcast{
							Data: ethTxBytes,
						}

						bytes, err := proto.Marshal(pluginBroadcast)

						if err != nil {
							panic(err)
						}

						mqClient.Broadcast("application/octet-stream", "plugin.bridge", bytes)
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
