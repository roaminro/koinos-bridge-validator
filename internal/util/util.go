package util

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcutil"
	log "github.com/koinos/koinos-log-golang"
	"github.com/mr-tron/base58"
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

func PublicKeyToAddress(pubkey *btcec.PublicKey) ([]byte, error) {
	mainNetAddr, _ := btcutil.NewAddressPubKey(pubkey.SerializeCompressed(), &chaincfg.MainNetParams)
	return base58.Decode(mainNetAddr.EncodeAddress())
}

func RecoverAddressFromSignature(signature string, hash []byte) (string, error) {
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	validatorPubKey, _, err := btcec.RecoverCompact(btcec.S256(), signatureBytes, hash[:])
	if err != nil {
		log.Error(err.Error())
		return "", err
	}
	validatorAddressBytes, err := PublicKeyToAddress(validatorPubKey)
	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	return base58.Encode(validatorAddressBytes), nil
}
