package properties

type Tls struct {
	CertFileLocation   string
	KeyFileLocation    string
	CaFileLocation     string
	InsecureSkipVerify bool
}
