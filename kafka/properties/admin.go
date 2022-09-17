package properties

import "time"

type Admin struct {
	BootstrapServers   []string
	ClientId           string
	SecurityProtocol   string
	Tls                *Tls
	CreateTopicTimeout time.Duration `default:"15s"`
}

func (p Admin) GetClientId() string {
	return p.ClientId
}

func (p Admin) GetSecurityProtocol() string {
	return p.SecurityProtocol
}

func (p Admin) GetTls() *Tls {
	return p.Tls
}
