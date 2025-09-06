package enum

type Error int

const (
	Error_NoDataFound Error = iota
)

func (index Error) String() string {
	return []string{
		"No data found.",
	}[index]
}
