package properties

type Admin struct {
	BootstrapServers []string
	ClientId         string
	SecurityProtocol string
	Tls              *Tls
}
