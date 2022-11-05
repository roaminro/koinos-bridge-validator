package rpc

import (
	"context"

	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	chainrpc "github.com/koinos/koinos-proto-golang/koinos/rpc/chain"
	kjsonrpc "github.com/koinos/koinos-util-golang/rpc"
	"github.com/multiformats/go-multihash"
)

// RPC service constants
const (
	GetHeadInfoCall       = "chain.get_head_info"
	GetBlocksByHeightCall = "block_store.get_blocks_by_height"
	SubmitBlockCall       = "chain.submit_block"
)

// JsonRPC
type JsonRPC struct {
	client *kjsonrpc.KoinosRPCClient
}

// NewJsonRPC factory
func NewJsonRPC(client *kjsonrpc.KoinosRPCClient) *JsonRPC {
	rpc := new(JsonRPC)
	rpc.client = client
	return rpc
}

func (k *JsonRPC) GetHeadInfo(ctx context.Context) (*chainrpc.GetHeadInfoResponse, error) {
	params := chainrpc.GetHeadInfoRequest{}

	headInfo := &chainrpc.GetHeadInfoResponse{}

	err := k.client.Call(ctx, GetHeadInfoCall, &params, headInfo)
	if err != nil {
		return nil, err
	}

	return headInfo, nil
}

func (k *JsonRPC) GetBlocksByHeight(ctx context.Context, blockID multihash.Multihash, height uint64, numBlocks uint32) (*block_store.GetBlocksByHeightResponse, error) {
	params := block_store.GetBlocksByHeightRequest{
		ReturnBlock:         true,
		ReturnReceipt:       true,
		NumBlocks:           numBlocks,
		AncestorStartHeight: height,
		HeadBlockId:         blockID,
	}

	blockResponse := &block_store.GetBlocksByHeightResponse{}

	err := k.client.Call(ctx, GetBlocksByHeightCall, &params, blockResponse)
	if err != nil {
		return nil, err
	}

	return blockResponse, nil
}

func (k *JsonRPC) ApplyBlock(ctx context.Context, block *protocol.Block) (*chainrpc.SubmitBlockResponse, error) {
	submitBlockResp := &chainrpc.SubmitBlockResponse{}
	err := k.client.Call(ctx, SubmitBlockCall, &chainrpc.SubmitBlockRequest{Block: block}, submitBlockResp)

	if err != nil {
		return nil, err
	}

	return submitBlockResp, nil
}
