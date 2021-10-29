package properties

type Consumer struct {
	BootstrapServers []string
	ClientId         string
	SecurityProtocol string
	Tls              *Tls
	InitialOffset    int64 `default:"-1"` // -1: Newest, -2: Oldest
}
