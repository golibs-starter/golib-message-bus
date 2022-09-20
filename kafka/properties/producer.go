package properties

import "time"

type Producer struct {
	BootstrapServers []string
	ClientId         string
	SecurityProtocol string
	Tls              *Tls
	FlushMessages    int           `default:"1"`
	FlushFrequency   time.Duration `default:"1s"`
}

func (p Producer) GetClientId() string {
	return p.ClientId
}

func (p Producer) GetSecurityProtocol() string {
	return p.SecurityProtocol
}

func (p Producer) GetTls() *Tls {
	return p.Tls
}
