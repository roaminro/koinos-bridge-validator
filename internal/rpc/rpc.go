package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/mr-tron/base58"
	"github.com/roaminroe/koinos-bridge-validator/internal/store"
	"github.com/roaminroe/koinos-bridge-validator/internal/util"
	"github.com/roaminroe/koinos-bridge-validator/proto/build/github.com/roaminroe/koinos-bridge-validator/bridge_pb"
	"google.golang.org/protobuf/proto"

	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	prpc "github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/p2p"
	rpcplugin "github.com/koinos/koinos-proto-golang/koinos/rpc/plugin"
)

// IsConnectedToP2P returns if the AMQP connection can currently communicate
// with the P2P microservice.
func IsConnectedToP2P(ctx context.Context, client *koinosmq.Client) (bool, error) {
	args := &p2p.P2PRequest{
		Request: &p2p.P2PRequest_Reserved{
			Reserved: &prpc.ReservedRpc{},
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

func P2PHandleRPC(
	rpcType string,
	data []byte,
	ethContractStr string,
	koinosContractStr string,
	ethTxStore *store.TransactionsStore,
	validators map[string]string,
) ([]byte, error) {
	// ethContractAddr := common.HexToAddress(ethContractStr)
	koinosContractAddr, err := base58.Decode(koinosContractStr)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	req := &rpcplugin.PluginRequest{}
	resp := &rpcplugin.PluginResponse{}

	err = proto.Unmarshal(data, req)
	if err != nil {
		log.Warnf("Received malformed request: 0x%v", hex.EncodeToString(data))
		log.Error(err.Error())
		return nil, err
	} else {
		if req.Request != nil {
			switch v := req.Request.(type) {
			case *rpcplugin.PluginRequest_SubmitData:
				transaction := &bridge_pb.Transaction{}

				err := proto.Unmarshal(v.SubmitData.Data, transaction)
				log.Debugf("Received RPC request: 0x%v", hex.EncodeToString(data))
				if err != nil {
					log.Warnf("Received malformed request: 0x%v", hex.EncodeToString(req.GetSubmitData().Data))
					eResp := prpc.ErrorResponse{Message: err.Error()}
					rErr := rpcplugin.PluginResponse_Error{Error: &eResp}
					resp.Response = &rErr
				} else {
					if transaction.Type == bridge_pb.TransactionType_ethereum {
						log.Infof("received Ethereum tx %s / validators: %+q / signatures: %+q", transaction.Id, transaction.Validators, transaction.Signatures)
						// check transaction hash
						txIdBytes := common.FromHex(transaction.Id)

						amount, err := strconv.ParseUint(transaction.Amount, 0, 64)
						if err != nil {
							log.Error(err.Error())
							return nil, err
						}

						koinosToken, err := base58.Decode(transaction.KoinosToken)
						if err != nil {
							log.Error(err.Error())
							return nil, err
						}
						recipient, err := base58.Decode(transaction.Recipient)
						if err != nil {
							log.Error(err.Error())
							return nil, err
						}

						completeTransferHash := &bridge_pb.CompleteTransferHash{
							Action:        bridge_pb.ActionId_complete_transfer,
							TransactionId: txIdBytes,
							Token:         koinosToken,
							Recipient:     recipient,
							Amount:        amount,
							ContractId:    koinosContractAddr,
						}

						completeTransferHashBytes, err := proto.Marshal(completeTransferHash)
						if err != nil {
							log.Error(err.Error())
							return nil, err
						}

						hash := sha256.Sum256(completeTransferHashBytes)
						hashB64 := base64.StdEncoding.EncodeToString(hash[:])

						if hashB64 != transaction.Hash {
							errMsg := fmt.Sprintf("the calulated hash for tx %s is different than the one received %s != calculated %s", transaction.Id, transaction.Hash, hashB64)
							log.Errorf(errMsg)
							return nil, fmt.Errorf(errMsg)
						} else if len(transaction.Validators) == len(transaction.Signatures) {
							// check signatures
							for index, signature := range transaction.Signatures {
								validatorReceived := transaction.Validators[index]

								_, found := validators[validatorReceived]
								if !found {
									errMsg := fmt.Sprintf("validator %s is not allowed", validatorReceived)
									log.Errorf(errMsg)
									return nil, fmt.Errorf(errMsg)
								}

								validatorCalculated, err := util.RecoverAddressFromSignature(signature, hash[:])
								if err != nil {
									log.Error(err.Error())
									return nil, err
								}

								if validatorReceived != validatorCalculated {
									errMsg := fmt.Sprintf("the signature provided for validator %s does not match the address recovered %s", validatorReceived, validatorCalculated)
									log.Errorf(errMsg)
									return nil, fmt.Errorf(errMsg)
								}
							}

							// check if we already have this transaction in our store
							ethTx, err := ethTxStore.Get(transaction.Id)
							if err != nil {
								panic(err)
							}

							if ethTx != nil {
								if ethTx.Hash != hashB64 {
									errMsg := fmt.Sprintf("the calculated hash for tx %s is different than the one received %s != calculated %s", transaction.Hash, ethTx.Hash, hashB64)

									log.Errorf(errMsg)
									return nil, fmt.Errorf(errMsg)
								}

								signatures := make(map[string]string)

								for index, validatr := range ethTx.Validators {
									signatures[validatr] = ethTx.Signatures[index]
								}

								for index, validatr := range transaction.Validators {
									_, found := signatures[validatr]
									if !found {
										signatures[validatr] = transaction.Signatures[index]
									}
								}

								ethTx.Validators = []string{}
								ethTx.Signatures = []string{}
								for val, sig := range signatures {
									ethTx.Validators = append(ethTx.Validators, val)
									ethTx.Signatures = append(ethTx.Signatures, sig)
								}
							} else {
								ethTx = transaction
							}

							err = ethTxStore.Put(ethTx.Id, ethTx)

							if err != nil {
								panic(err)
							}
						}
					}
				}
			}
		}
	}

	var outputBytes []byte
	outputBytes, err = proto.Marshal(resp)

	return outputBytes, err
}

func HandleRPC(rpcType string, data []byte, ethTxStore *store.TransactionsStore) ([]byte, error) {
	request := &bridge_pb.BridgeRequest{}
	response := &bridge_pb.BridgeResponse{}

	err := proto.Unmarshal(data, request)

	if err != nil {
		log.Warnf("Received malformed request: %v", data)
	} else {
		log.Debugf("Received RPC request: %s", request.String())
		switch v := request.Request.(type) {
		case *bridge_pb.BridgeRequest_GetEthereumTransaction:
			if transaction, err := ethTxStore.Get(v.GetEthereumTransaction.TransactionId); err == nil {
				r := &bridge_pb.GetEthereumTransactionResponse{Transaction: transaction}
				response.Response = &bridge_pb.BridgeResponse_GetEthereumTransaction{GetEthereumTransaction: r}
			}
		default:
			err = errors.New("unknown request")
		}
	}

	if err != nil {
		e := &rpc.ErrorResponse{Message: string(err.Error())}
		response.Response = &bridge_pb.BridgeResponse_Error{Error: e}
	}

	return proto.Marshal(response)
}
