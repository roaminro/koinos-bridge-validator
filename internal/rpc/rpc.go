package rpc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"google.golang.org/protobuf/proto"

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

func HandleRPC(rpcType string, data []byte) ([]byte, error) {
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
