package pkg

import "os"

type Creds struct {
	Url, IntegrationID, Password string
}

func LoadCreds() *Creds {
	return &Creds{
		Url:           os.Getenv("APIURL"),
		IntegrationID: os.Getenv("CLARIFY_INTEGRATION_ID"),
		Password:      os.Getenv("CLARIFY_PASSWORD"),
	}
}
