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
