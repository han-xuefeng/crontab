package worker

import (
	"encoding/json"
	"io/ioutil"
)

var (
	G_config *Config
)

type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json"jobLogCommitTimeout"`
	BinBash string `json:"binBash"`
}

func InitConfig(confFile string) (err error) {
	var (
		configBytes []byte
		config Config
	)
	if configBytes ,err = ioutil.ReadFile(confFile);err != nil{
		return
	}
	if err = json.Unmarshal(configBytes,&config);err != nil{
		return
	}

	G_config =&config
	return
}
