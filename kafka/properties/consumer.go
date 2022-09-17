package properties

type Consumer struct {
	BootstrapServers []string
	ClientId         string
	SecurityProtocol string
	Tls              *Tls
	InitialOffset    int64 `default:"-1"` // -1: Newest, -2: Oldest
}

func (p Consumer) GetClientId() string {
	return p.ClientId
}

func (p Consumer) GetSecurityProtocol() string {
	return p.SecurityProtocol
}

func (p Consumer) GetTls() *Tls {
	return p.Tls
}
