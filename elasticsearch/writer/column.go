package elasticsearch

type EsColumn struct {
	Name                        string
	Type                        string
	Timezone                    string
	Format                      string
	DstFormat                   string
	Array                       bool
	DstArray                    bool
	JsonArray                   bool
	Origin                      bool
	CombineFields               []string
	CombineFieldsValueSeparator string
}

func DefaultColumn() *EsColumn {
	return &EsColumn{
		DstArray:                    false,
		CombineFieldsValueSeparator: "-",
	}
}
