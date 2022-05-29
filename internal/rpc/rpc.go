package rpc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/roaminroe/koinos-bridge-validator/internal/store"
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

func P2PHandleRPC(rpcType string, data []byte) ([]byte, error) {
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
		// TODO: handle RPC request
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
