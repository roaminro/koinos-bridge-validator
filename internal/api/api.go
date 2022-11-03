package api

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/koinos/koinos-log-golang"
	"github.com/mr-tron/base58"

	"net/http"

	"github.com/roaminroe/koinos-bridge-validator/internal/store"
	"github.com/roaminroe/koinos-bridge-validator/internal/util"
	"github.com/roaminroe/koinos-bridge-validator/proto/build/github.com/roaminroe/koinos-bridge-validator/bridge_pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Api struct {
	ethTxStore            *store.TransactionsStore
	koinosTxStore         *store.TransactionsStore
	koinosContractAddress []byte
	ethContractAddress    common.Address
	validators            map[string]string
}

func NewApi(ethTxStore *store.TransactionsStore, koinosTxStore *store.TransactionsStore, koinosContractStr string, ethContractStr string, validators map[string]string) *Api {
	ethContractAddress := common.HexToAddress(ethContractStr)

	koinosContractAddress, err := base58.Decode(koinosContractStr)
	if err != nil {
		panic(err)
	}

	return &Api{
		ethTxStore:            ethTxStore,
		koinosTxStore:         koinosTxStore,
		koinosContractAddress: koinosContractAddress,
		ethContractAddress:    ethContractAddress,
		validators:            validators,
	}
}

func (api *Api) GetEthereumTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad Request"))
		return
	}

	transactionIdParams := r.URL.Query()["TransactionId"]

	if len(transactionIdParams) <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing TransactionId param"))
		return
	}

	transaction, _ := api.ethTxStore.Get(transactionIdParams[0])

	if transaction == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("transaction does not exist"))
		return
	}

	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	jsonBytes, err := m.Marshal(transaction)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("unknown error"))
		log.Error(err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonBytes)
}

func (api *Api) GetKoinosTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad Request"))
		return
	}

	transactionIdParams := r.URL.Query()["TransactionId"]

	if len(transactionIdParams) <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing TransactionId param"))
		return
	}

	transaction, _ := api.koinosTxStore.Get(transactionIdParams[0])

	if transaction == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("transaction does not exist"))
		return
	}

	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	jsonBytes, err := m.Marshal(transaction)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("unknown error"))
		log.Error(err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonBytes)
}

func (api *Api) SubmitSignature(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad Request"))
		return
	}

	var transaction bridge_pb.Transaction
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid body"))
		return
	}

	println(body)

	err = protojson.Unmarshal(body, &transaction)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid transaction json"))
		return
	}

	if transaction.Type == bridge_pb.TransactionType_ethereum {
		log.Infof("received Ethereum tx %s / validators: %+q / signatures: %+q", transaction.Id, transaction.Validators, transaction.Signatures)
		// check transaction hash
		txIdBytes := common.FromHex(transaction.Id)

		amount, err := strconv.ParseUint(transaction.Amount, 0, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid amount"))
			return
		}

		koinosToken, err := base58.Decode(transaction.KoinosToken)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid koinosToken"))
			return
		}

		recipient, err := base58.Decode(transaction.Recipient)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid recipient"))
			return
		}

		completeTransferHash := &bridge_pb.CompleteTransferHash{
			Action:        bridge_pb.ActionId_complete_transfer,
			TransactionId: txIdBytes,
			Token:         koinosToken,
			Recipient:     recipient,
			Amount:        amount,
			ContractId:    api.koinosContractAddress,
			Expiration:    transaction.Expiration,
		}

		completeTransferHashBytes, err := proto.Marshal(completeTransferHash)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid completeTransferHash"))
			return
		}

		hash := sha256.Sum256(completeTransferHashBytes)
		hashB64 := base64.StdEncoding.EncodeToString(hash[:])

		if hashB64 != transaction.Hash {
			errMsg := fmt.Sprintf("the calulated hash for tx %s is different than the one received %s != calculated %s", transaction.Id, transaction.Hash, hashB64)
			log.Errorf(errMsg)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(errMsg))
			return
		}

		if len(transaction.Validators) == len(transaction.Signatures) {
			// check signatures
			for index, signature := range transaction.Signatures {
				validatorReceived := transaction.Validators[index]

				_, found := api.validators[validatorReceived]
				if !found {
					errMsg := fmt.Sprintf("validator %s is not allowed", validatorReceived)
					log.Errorf(errMsg)
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(errMsg))
					return
				}

				validatorCalculated, err := util.RecoverAddressFromSignature(signature, hash[:])
				if err != nil {
					log.Error(err.Error())
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte("cannot recover signature"))
					return
				}

				if validatorReceived != validatorCalculated {
					errMsg := fmt.Sprintf("the signature provided for validator %s does not match the address recovered %s", validatorReceived, validatorCalculated)
					log.Errorf(errMsg)
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(errMsg))
					return
				}
			}

			// check if we already have this transaction in our store
			api.ethTxStore.Lock()
			ethTx, err := api.ethTxStore.Get(transaction.Id)
			if err != nil {
				log.Errorf(err.Error())
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("error while getting transaction"))
				api.ethTxStore.Unlock()
				return
			}

			if ethTx != nil {
				if ethTx.Hash != hashB64 {
					errMsg := fmt.Sprintf("the calculated hash for tx %s is different than the one received %s != calculated %s", transaction.Hash, ethTx.Hash, hashB64)

					log.Errorf(errMsg)
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(errMsg))
					api.ethTxStore.Unlock()
					return
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
				// TODO: check if other values in transaction are legit before saving it
				ethTx = &transaction
			}

			err = api.ethTxStore.Put(ethTx.Id, ethTx)
			api.ethTxStore.Unlock()

			if err != nil {
				log.Errorf(err.Error())
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("error while saving transaction"))
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("signature successfully processed"))
		}
	}
}
