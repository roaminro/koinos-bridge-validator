package util

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/btcec"
	"gopkg.in/yaml.v2"
)

type YamlConfig struct {
	Global            map[string]interface{} `yaml:"global,omitempty"`
	P2P               map[string]interface{} `yaml:"p2p,omitempty"`
	BlockStore        map[string]interface{} `yaml:"block_store,omitempty"`
	JSONRPC           map[string]interface{} `yaml:"jsonrpc,omitempty"`
	TransactionStore  map[string]interface{} `yaml:"transaction_store,omitempty"`
	ContractMetaStore map[string]interface{} `yaml:"contract_meta_store,omitempty"`
	Bridge            map[string]interface{} `yaml:"bridge,omitempty"`
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
	} else {
		yamlConfig.Global = make(map[string]interface{})
		yamlConfig.P2P = make(map[string]interface{})
		yamlConfig.BlockStore = make(map[string]interface{})
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
