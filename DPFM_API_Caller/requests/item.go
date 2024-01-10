package requests

type Item struct {
	Contract           int     `json:"Contract"`
	ContractItem       int     `json:"ContractItem"`
	IsCancelled        *bool   `json:"IsCancelled"`
}
