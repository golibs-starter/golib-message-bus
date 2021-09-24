package properties

type Consumer struct {
	BootstrapServers []string
	ClientId         string
	SecurityProtocol string
	Tls              *Tls
}
