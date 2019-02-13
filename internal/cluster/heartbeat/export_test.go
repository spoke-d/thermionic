package heartbeat

import "github.com/go-kit/kit/log"

func NewHeartbeatWithMocks(
	gateway Gateway,
	cluster Cluster,
	task Task,
	certConfig CertConfig,
) *Heartbeat {
	return &Heartbeat{
		gateway:          gateway,
		cluster:          cluster,
		task:             task,
		certConfig:       certConfig,
		databaseEndpoint: DatabaseEndpoint,
		logger:           log.NewNopLogger(),
	}
}
