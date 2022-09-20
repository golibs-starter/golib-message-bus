package properties

type Consumer struct {
	BootstrapServers []string
	ClientId         string
	SecurityProtocol string
	Tls              *Tls
	InitialOffset    int64  `default:"-1"` // -1: Newest, -2: Oldest
	CommitMode       string `default:"AUTO_COMMIT_INTERVAL" validate:"required=false,oneof=AUTO_COMMIT_INTERVAL AUTO_COMMIT_IMMEDIATELY"`
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
