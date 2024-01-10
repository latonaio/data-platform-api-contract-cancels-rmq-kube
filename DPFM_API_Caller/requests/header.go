package requests

type Header struct {
	Contract             int     `json:"Contract"`
	IsCancelled          *bool   `json:"IsCancelled"`
}
