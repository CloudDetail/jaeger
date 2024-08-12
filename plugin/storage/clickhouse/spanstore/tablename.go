package spanstore

type TableName string

func (t TableName) ToLocal() TableName {
	return t + "_local"
}
