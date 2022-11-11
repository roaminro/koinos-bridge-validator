package util

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/koinos-bridge/koinos-bridge-validator/proto/build/github.com/koinos-bridge/koinos-bridge-validator/bridge_pb"
	log "github.com/koinos/koinos-log-golang"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v2"
)

type ValidatorConfig struct {
	EthereumAddress string `yaml:"ethereum-address"`
	KoinosAddress   string `yaml:"koinos-address"`
	ApiUrl          string `yaml:"api-url"`
}

type TokenConfig struct {
	EthereumAddress string `yaml:"ethereum-address"`
	KoinosAddress   string `yaml:"koinos-address"`
}

type BridgeConfig struct {
	Reset                bool   `yaml:"reset"`
	InstanceID           string `yaml:"instance-id"`
	LogLevel             string `yaml:"log-level"`
	SignaturesExpiration uint   `yaml:"signatures-expiration"`
	ApiUrl               string `yaml:"api-url"`

	EthereumRpc             string `yaml:"ethereum-rpc"`
	EthereumContract        string `yaml:"ethereum-contract"`
	EthereumBlockStart      string `yaml:"ethereum-block-start"`
	EthereumPK              string `yaml:"ethereum-pk"`
	EthereumMaxBlocksStream string `yaml:"ethereum-max-blocks-stream"`
	EthereumConfirmations   string `yaml:"ethereum-confirmations"`

	KoinosRpc             string `yaml:"koinos-rpc"`
	KoinosContract        string `yaml:"koinos-contract"`
	KoinosBlockStart      string `yaml:"koinos-block-start"`
	KoinosPK              string `yaml:"koinos-pk"`
	KoinosMaxBlocksStream string `yaml:"koinos-max-blocks-stream"`

	Validators map[string]ValidatorConfig `yaml:"validators"`
	Tokens     map[string]TokenConfig     `yaml:"tokens"`
}

type YamlConfig struct {
	Global map[string]interface{} `yaml:"global,omitempty"`
	Bridge BridgeConfig           `yaml:"bridge"`
}

// InitYamlConfig initializes a yaml config
func InitYamlConfig(baseDir string) *YamlConfig {
	yamlConfigPath := filepath.Join(baseDir, "config.yml")
	if _, err := os.Stat(yamlConfigPath); os.IsNotExist(err) {
		yamlConfigPath = filepath.Join(baseDir, "config.yaml")
	}

	yamlConfig := YamlConfig{}

	if _, err := os.Stat(yamlConfigPath); err == nil {
		data, err := ioutil.ReadFile(yamlConfigPath)
		if err != nil {
			panic(err)
		}

		err = yaml.Unmarshal(data, &yamlConfig)
		if err != nil {
			panic(err)
		}
	}

	return &yamlConfig
}

func SignKoinosHash(key []byte, hash []byte) []byte {
	privateKey, _ := btcec.PrivKeyFromBytes(btcec.S256(), key)

	// Sign the hash
	signatureBytes, err := btcec.SignCompact(btcec.S256(), privateKey, hash, true)
	if err != nil {
		panic(err)
	}

	return signatureBytes
}

func SignEthereumHash(key *ecdsa.PrivateKey, hash []byte) []byte {
	signatureBytes, err := crypto.Sign(hash, key)

	if err != nil {
		panic(err)
	}
	signatureBytes[crypto.RecoveryIDOffset] += 27

	return signatureBytes
}

func GetStringOption(a string, b string) string {
	if a != "" {
		return a
	} else {
		return b
	}
}

func GetUIntOption(a uint, b uint) uint {
	if a != 0 {
		return a
	} else {
		return b
	}
}

func GetBoolOption(a bool, b bool) bool {
	if a {
		return a
	} else {
		return b
	}
}

func KoinosPublicKeyToAddress(pubkey *btcec.PublicKey) ([]byte, error) {
	mainNetAddr, _ := btcutil.NewAddressPubKey(pubkey.SerializeCompressed(), &chaincfg.MainNetParams)
	return base58.Decode(mainNetAddr.EncodeAddress())
}

func RecoverEthereumAddressFromSignature(signature string, prefixedHash []byte) (string, error) {
	signatureBytes := common.Hex2Bytes(signature[2:])

	signatureBytes[crypto.RecoveryIDOffset] -= 27 // Transform yellow paper V from 27/28 to 0/1

	recovered, err := crypto.SigToPub(prefixedHash, signatureBytes)
	if err != nil {
		return "", err
	}

	recoveredAddr := crypto.PubkeyToAddress(*recovered).Hex()

	return recoveredAddr, nil
}

func RecoverKoinosAddressFromSignature(signature string, hash []byte) (string, error) {
	signatureBytes, err := base64.URLEncoding.DecodeString(signature)
	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	validatorPubKey, _, err := btcec.RecoverCompact(btcec.S256(), signatureBytes, hash[:])
	if err != nil {
		log.Error(err.Error())
		return "", err
	}
	validatorAddressBytes, err := KoinosPublicKeyToAddress(validatorPubKey)
	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	return base58.Encode(validatorAddressBytes), nil
}

func BroadcastTransaction(tx *bridge_pb.Transaction, koinosPK []byte, koinosAddress string, validators map[string]ValidatorConfig) (map[string]string, error) {
	signatures := make(map[string]string)

	txBytes, err := proto.Marshal(tx)
	if err != nil {
		return nil, err
	}

	hash := sha256.Sum256(txBytes)
	sigBytes := SignKoinosHash(koinosPK, hash[:])
	sigB64 := base64.URLEncoding.EncodeToString(sigBytes)

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

		if res.StatusCode == http.StatusOK {
			signatureBytes, _ := ioutil.ReadAll(res.Body)
			signature := string(signatureBytes)

			if signature != "" {
				log.Infof("client: received signature %s\n", signatureBytes)
				signatures[validator.KoinosAddress] = signature
			}
		}

		processedApiUrls[validator.ApiUrl] = true
	}

	return signatures, nil
}

func GenerateEthereumCompleteTransferHash(txIdBytes []byte, operationId uint64, ethToken []byte, recipient []byte, amountStr string, ethContractAddress common.Address, expiration uint64) (common.Hash, common.Hash) {
	amount, err := strconv.ParseUint(amountStr, 0, 64)
	if err != nil {
		panic(err)
	}
	hash := crypto.Keccak256Hash(
		common.LeftPadBytes(big.NewInt(int64(bridge_pb.ActionId_complete_transfer.Number())).Bytes(), 32),
		txIdBytes,
		common.LeftPadBytes(big.NewInt(int64(operationId)).Bytes(), 32),
		ethToken,
		recipient,
		common.LeftPadBytes(big.NewInt(int64(amount)).Bytes(), 32),
		ethContractAddress.Bytes(),
		common.LeftPadBytes(big.NewInt(int64(expiration)).Bytes(), 32),
	)

	prefixedHash := crypto.Keccak256Hash(
		[]byte(fmt.Sprintf("\x19Ethereum Signed Message:\n%v", len(hash))),
		hash.Bytes(),
	)

	return hash, prefixedHash
}
