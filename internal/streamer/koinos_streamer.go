package streamer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	kjsonrpc "github.com/koinos/koinos-util-golang/rpc"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/proto"

	"github.com/koinos-bridge/koinos-bridge-validator/internal/rpc"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/store"
	"github.com/koinos-bridge/koinos-bridge-validator/internal/util"
	"github.com/koinos-bridge/koinos-bridge-validator/proto/build/github.com/koinos-bridge/koinos-bridge-validator/bridge_pb"
)

func StreamKoinosBlocks(
	ctx context.Context,
	metadataStore *store.MetadataStore,
	savedLastKoinosBlockParsed string,
	koinosRPC string,
	ethereumPK *ecdsa.PrivateKey,
	ethereumAddress string,
	ethContractStr string,
	koinosMaxBlocksToStreamStr string,
	koinosPK []byte,
	koinosAddress string,
	koinosContractStr string,
	tokenAddresses map[string]util.TokenConfig,
	ethTxStore *store.TransactionsStore,
	koinosTxStore *store.TransactionsStore,
	signaturesExpiration uint,
	validators map[string]util.ValidatorConfig,
) {
	// init JSON RPC client
	rpcCl := kjsonrpc.NewKoinosRPCClient(koinosRPC)
	rpcClient := rpc.NewJsonRPC(rpcCl)

	fmt.Println("connected to Koinos RPC")

	koinosMaxBlocksToStream, err := strconv.ParseUint(koinosMaxBlocksToStreamStr, 0, 64)
	if err != nil {
		panic(err)
	}

	startBlock, err := strconv.ParseUint(savedLastKoinosBlockParsed, 0, 64)
	if err != nil {
		panic(err)
	}

	startBlock++

	ethContractAddr := common.HexToAddress(ethContractStr)
	koinosContractAddr, err := base58.Decode(koinosContractStr)
	if err != nil {
		panic(err)
	}

	var lastKoinosBlockParsed uint64
	fromBlock := startBlock

	for {
		select {
		case <-ctx.Done():
			log.Infof("stop streaming blocks %d", lastKoinosBlockParsed)
			metadataStore.Lock()
			defer metadataStore.Unlock()

			metadata, err := metadataStore.Get()
			if err != nil {
				panic(err)
			}

			metadata.LastKoinosBlockParsed = strconv.FormatUint(lastKoinosBlockParsed, 10)

			metadataStore.Put(metadata)
			return

		case <-time.After(time.Millisecond * 1000):
			headInfo, err := rpcClient.GetHeadInfo(ctx)

			if err != nil {
				if !strings.Contains(err.Error(), "context canceled") {
					panic(err)
				}
			} else {
				log.Infof("last irreversible block: %d", headInfo.LastIrreversibleBlock)

				var nbBlocksToFetch uint64 = 0

				if headInfo.LastIrreversibleBlock > fromBlock {
					nbBlocksToFetch = headInfo.LastIrreversibleBlock - fromBlock
				}

				if nbBlocksToFetch > koinosMaxBlocksToStream {
					nbBlocksToFetch = koinosMaxBlocksToStream
				}

				var toBlock = fromBlock + nbBlocksToFetch

				if toBlock <= headInfo.LastIrreversibleBlock {
					// get blocks
					blocks, err := rpcClient.GetBlocksByHeight(ctx, headInfo.HeadTopology.Id, fromBlock, uint32(nbBlocksToFetch))
					if err != nil {
						log.Error(err.Error())
						if !strings.Contains(err.Error(), "context canceled") {
							panic(err)
						}
					} else {
						log.Infof("fetched koinos blocks: %d - %d", fromBlock, toBlock)

						for _, block := range blocks.BlockItems {
							log.Infof("block#: %d", block.BlockHeight)
							for _, receipt := range block.Receipt.TransactionReceipts {
								// make the sure the transaction did not revert
								if !receipt.Reverted {
									// check each events
									for _, event := range receipt.Events {
										if bytes.Equal(event.Source, koinosContractAddr) {
											if event.Name == "bridge.tokens_locked_event" {
												processKoinosTokensLockedEvent(
													ethereumPK,
													ethereumAddress,
													koinosPK,
													koinosAddress,
													ethContractAddr,
													tokenAddresses,
													koinosTxStore,
													signaturesExpiration,
													validators,
													block,
													receipt,
													event,
												)
											} else if event.Name == "bridge.transfer_completed_event" {
												processKoinosTransferCompletedEvent(
													koinosTxStore,
													block,
													receipt,
													event,
												)
											}
										}
									}
								}
							}

							lastKoinosBlockParsed = block.BlockHeight
						}

						if len(blocks.BlockItems) > 0 {
							fromBlock = lastKoinosBlockParsed + 1
						}
					}
				} else {
					log.Info("waiting for new block: " + fmt.Sprint(fromBlock))
				}
			}
		}
	}
}

func processKoinosTransferCompletedEvent(
	ethTxStore *store.TransactionsStore,
	block *block_store.BlockItem,
	receipt *protocol.TransactionReceipt,
	event *protocol.EventData,
) {
	// parse event
	transferCompletedEvent := &bridge_pb.TransferCompletedEvent{}

	err := proto.Unmarshal(event.Data, transferCompletedEvent)
	if err != nil {
		panic(err)
	}

	blockNumber := block.BlockHeight
	ethTxId := common.Bytes2Hex(transferCompletedEvent.TxId)
	koinosTxId := common.Bytes2Hex(receipt.Id)
	koinosOpId := fmt.Sprint(event.Sequence)

	log.Infof("new Koinos transfer_completed_event | block: %s | eth tx: %s | koinos tx: %s | koinos op: %s", blockNumber, ethTxId, koinosTxId, koinosOpId)

	ethTxStore.Lock()
	ethTx, err := ethTxStore.Get(ethTxId)
	if err != nil {
		panic(err)
	}

	if ethTx == nil {
		log.Warnf("ethereum transaction %s does not exist", ethTx)
		ethTx = &bridge_pb.Transaction{}

	}

	ethTx.Status = bridge_pb.TransactionStatus_completed
	ethTx.CompletionTransactionId = koinosTxId + "-" + koinosOpId

	err = ethTxStore.Put(ethTxId, ethTx)
	if err != nil {
		panic(err)
	}
	ethTxStore.Unlock()
}

func processKoinosTokensLockedEvent(
	ethPK *ecdsa.PrivateKey,
	ethereumAddress string,
	koinosPK []byte,
	koinosAddress string,
	ethereumContractAddr common.Address,
	tokenAddresses map[string]util.TokenConfig,
	koinosTxStore *store.TransactionsStore,
	signaturesExpiration uint,
	validators map[string]util.ValidatorConfig,
	block *block_store.BlockItem,
	receipt *protocol.TransactionReceipt,
	event *protocol.EventData,
) {
	tokensLockedEvent := &bridge_pb.TokensLockedEvent{}

	err := proto.Unmarshal(event.Data, tokensLockedEvent)
	if err != nil {
		panic(err)
	}

	blockNumber := block.BlockHeight
	txId := receipt.Id
	txIdHex := "0x" + common.Bytes2Hex(receipt.Id)
	operationId := event.Sequence
	operationIdStr := fmt.Sprint(operationId)
	from := base58.Encode(tokensLockedEvent.From)
	koinosToken := base58.Encode(tokensLockedEvent.Token)
	amount := tokensLockedEvent.Amount
	recipient := common.HexToAddress(tokensLockedEvent.Recipient)
	blocktime := block.Block.Header.Timestamp
	amountStr := fmt.Sprint(tokensLockedEvent.Amount)

	ethereumToken := common.HexToAddress(tokenAddresses[koinosToken].EthereumAddress)

	log.Infof("new Koinos tokens_locked_event | block: %s | tx: %s | op_id: %s | Koinos token: %s | Ethereum token: %s | From: %s | recipient: %s | amount: %s ", blockNumber, txIdHex, operationIdStr, koinosToken, tokenAddresses[koinosToken].EthereumAddress, from, tokensLockedEvent.Recipient, amountStr)

	expiration := blocktime + uint64(signaturesExpiration)

	// sign the transaction
	_, prefixedHash := util.GenerateEthereumCompleteTransferHash(txId, uint64(operationId), ethereumToken.Bytes(), recipient.Bytes(), amount, ethereumContractAddr, expiration)

	sigBytes := util.SignEthereumHash(ethPK, prefixedHash.Bytes())
	sigHex := "0x" + common.Bytes2Hex(sigBytes)

	// store the transaction
	koinosTxStore.Lock()

	txKey := txIdHex + "-" + operationIdStr
	koinosTx, err := koinosTxStore.Get(txKey)
	if err != nil {
		panic(err)
	}

	if koinosTx == nil {
		koinosTx = &bridge_pb.Transaction{}
		koinosTx.Validators = []string{ethereumAddress}
		koinosTx.Signatures = []string{sigHex}
	} else {
		if koinosTx.Hash != "" && koinosTx.Hash != prefixedHash.Hex() {
			errMsg := fmt.Sprintf("the calculated hash for tx %s is different than the one already received %s != calculated %s", txIdHex, koinosTx.Hash, prefixedHash.Hex())
			log.Errorf(errMsg)
			panic(fmt.Errorf(errMsg))
		}
		koinosTx.Validators = append(koinosTx.Validators, ethereumAddress)
		koinosTx.Signatures = append(koinosTx.Signatures, sigHex)
	}

	koinosTx.Type = bridge_pb.TransactionType_koinos
	koinosTx.Id = txIdHex
	koinosTx.OpId = operationIdStr
	koinosTx.From = from
	koinosTx.EthToken = tokenAddresses[koinosToken].EthereumAddress
	koinosTx.KoinosToken = koinosToken
	koinosTx.Amount = amountStr
	koinosTx.Recipient = recipient.Hex()
	koinosTx.Hash = prefixedHash.Hex()
	koinosTx.BlockNumber = blockNumber
	koinosTx.BlockTime = blocktime
	koinosTx.Expiration = expiration

	if koinosTx.Status != bridge_pb.TransactionStatus_completed {
		koinosTx.Status = bridge_pb.TransactionStatus_gathering_signatures
	}

	err = koinosTxStore.Put(txKey, koinosTx)

	if err != nil {
		panic(err)
	}

	koinosTxStore.Unlock()

	// broadcast transaction
	koinoSignatures, _ := util.BroadcastTransaction(koinosTx, koinosPK, koinosAddress, validators)

	// the signatures received from the broadcast are mapped using the Koinos validators addresses
	// remap to Ethereum addresses
	ethSignatures := make(map[string]string)
	for val, sig := range koinoSignatures {
		ethSignatures[validators[val].EthereumAddress] = sig
	}

	// update the transaction with signatures we may have gotten back from the broadcast
	koinosTxStore.Lock()

	koinosTx, err = koinosTxStore.Get(txKey)
	if err != nil {
		panic(err)
	}

	// add signatures we may already have
	for index, validatr := range koinosTx.Validators {
		_, found := ethSignatures[validatr]
		if !found {
			ethSignatures[validatr] = koinosTx.Signatures[index]
		}
	}

	koinosTx.Validators = []string{}
	koinosTx.Signatures = []string{}
	for val, sig := range ethSignatures {
		koinosTx.Validators = append(koinosTx.Validators, val)
		koinosTx.Signatures = append(koinosTx.Signatures, sig)
	}

	if koinosTx.Status != bridge_pb.TransactionStatus_completed &&
		len(koinosTx.Signatures) >= ((((len(validators)/2)*10)/3)*2)/10+1 {
		koinosTx.Status = bridge_pb.TransactionStatus_signed
	}

	err = koinosTxStore.Put(txKey, koinosTx)

	if err != nil {
		panic(err)
	}

	koinosTxStore.Unlock()
}
