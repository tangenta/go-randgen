package sqlgen

type ColumnType int64

const (
	ColumnTypeInt ColumnType = iota
	ColumnTypeBoolean
	ColumnTypeTinyInt
	ColumnTypeSmallInt
	ColumnTypeMediumInt
	ColumnTypeBigInt
	ColumnTypeFloat
	ColumnTypeDouble
	ColumnTypeDecimal
	ColumnTypeBit

	ColumnTypeChar
	ColumnTypeVarchar
	ColumnTypeText
	ColumnTypeBlob
	ColumnTypeBinary
	ColumnTypeEnum
	ColumnTypeSet

	ColumnTypeDate
	ColumnTypeTime
	ColumnTypeDatetime
	ColumnTypeTimestamp

	ColumnTypeMax
)

func (c ColumnType) IsStringType() bool {
	switch c {
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText,
		ColumnTypeBlob, ColumnTypeBinary:
		return true
	}
	return false
}

func (c ColumnType) NeedKeyLength() bool {
	return c == ColumnTypeBlob || c == ColumnTypeText
}

func (c ColumnType) IsIntegerType() bool {
	switch c {
	case ColumnTypeInt, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeBigInt:
		return true
	}
	return false
}

// BLOB/TEXT/JSON column can't have a default value.
func (c ColumnType) DisallowDefaultValue() bool {
	return c == ColumnTypeText || c == ColumnTypeBlob
}

func (c ColumnType) String() string {
	switch c {
	case ColumnTypeInt:
		return "int"
	case ColumnTypeBoolean:
		return "boolean"
	case ColumnTypeTinyInt:
		return "tinyint"
	case ColumnTypeSmallInt:
		return "smallint"
	case ColumnTypeMediumInt:
		return "mediumint"
	case ColumnTypeBigInt:
		return "bigint"
	case ColumnTypeFloat:
		return "float"
	case ColumnTypeDouble:
		return "double"
	case ColumnTypeDecimal:
		return "decimal"
	case ColumnTypeBit:
		return "bit"
	case ColumnTypeChar:
		return "char"
	case ColumnTypeVarchar:
		return "varchar"
	case ColumnTypeText:
		return "text"
	case ColumnTypeBlob:
		return "blob"
	case ColumnTypeBinary:
		return "binary"
	case ColumnTypeEnum:
		return "enum"
	case ColumnTypeSet:
		return "set"
	case ColumnTypeDate:
		return "date"
	case ColumnTypeTime:
		return "time"
	case ColumnTypeDatetime:
		return "datetime"
	case ColumnTypeTimestamp:
		return "timestamp"
	}
	return "invalid type"
}

type IndexType int64

const (
	IndexTypeNonUnique IndexType = iota
	IndexTypeUnique
	IndexTypePrimary

	IndexTypeMax
)

type ScopeKeyType int8

const (
	ScopeKeyCurrentTable ScopeKeyType = iota
	ScopeKeyCurrentColumn
	ScopeKeyCurrentIndex

	ScopeKeySelectedCols

	ScopeKeyTableUniqID
	ScopeKeyColumnUniqID
	ScopeKeyIndexUniqID
)
