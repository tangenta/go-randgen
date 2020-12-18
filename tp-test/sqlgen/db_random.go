package sqlgen

import (
	"fmt"
	"github.com/cznic/mathutil"
	"math/rand"
	"strconv"
	"time"
)

var (
	globalTableCounter  int = 0
	globalColumnCounter int = 0
	globalIndexCounter  int = 0
)

func NewRandomTable(columnNumber int) *Table {
	rand.Seed(time.Now().UnixNano())
	globalTableCounter++
	columns := make([]*Column, 0, columnNumber)
	for i := 0; i < columnNumber; i++ {
		columns = append(columns, NewRandomColumn())
	}
	return &Table{
		name:    fmt.Sprintf("tbl_%d", globalTableCounter),
		columns: columns,
	}
}

func NewRandomColumn() *Column {
	globalColumnCounter++
	name := fmt.Sprintf("col_%d", globalColumnCounter)
	tp := ColumnType(rand.Intn(int(ColumnTypeMax)))
	var (
		arg1, arg2 int
		args       []string
		isUnsigned bool
	)
	switch tp {
	// https://docs.pingcap.com/tidb/stable/data-type-numeric
	case ColumnTypeFloat | ColumnTypeDouble:
		arg1 = rand.Intn(256)
		upper := mathutil.Min(arg1, 30)
		arg2 = rand.Intn(upper + 1)
	case ColumnTypeDecimal:
		arg1 = rand.Intn(66)
		upper := mathutil.Min(arg1, 30)
		arg2 = rand.Intn(upper + 1)
	case ColumnTypeBit:
		arg1 = 1 + rand.Intn(64)
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeBinary:
		arg1 = 1 + rand.Intn(4294967295)
	case ColumnTypeEnum, ColumnTypeSet:
		args = []string{"Alice", "Bob", "Charlie", "David"}
	}
	if tp.IsIntegerType() {
		if RandomBool() {
			isUnsigned = true
		}
	}
	return &Column{name, tp, isUnsigned, arg1, arg2, args}
}

func (c *Column) RandomValue() string {
	if c.isUnsigned {
		switch c.tp {
		case ColumnTypeTinyInt:
			return RandomNum(0, 255)
		case ColumnTypeSmallInt:
			return RandomNum(0, 65535)
		case ColumnTypeMediumInt:
			return RandomNum(0, 16777215)
		case ColumnTypeInt:
			return RandomNum(0, 4294967295)
		case ColumnTypeBigInt:
			return RandomNum(0, 9223372036854775806)
		}
	}
	switch c.tp {
	case ColumnTypeTinyInt:
		return RandomNum(-128, 127)
	case ColumnTypeSmallInt:
		return RandomNum(-32768, 32767)
	case ColumnTypeMediumInt:
		return RandomNum(-8388608, 8388607)
	case ColumnTypeInt:
		return RandomNum(-2147483648, 2147483647)
	case ColumnTypeBigInt:
		num := rand.Int63()
		if RandomBool() {
			num = -num
		}
		return strconv.FormatInt(num, 10)
	case ColumnTypeBoolean:
		return RandomNum(0, 1)
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal:
		return RandomFloat(0, 3.402823466e+38)
	case ColumnTypeBit:
		return RandomNum(0, (1<<c.arg1)-1)
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeBinary:
		length := c.arg1
		if length == 0 {
			length = 5
		} else if length > 20 {
			length = 20
		}
		return fmt.Sprintf("'%s'", RandStringRunes(rand.Intn(length)))
	case ColumnTypeEnum, ColumnTypeSet:
		return fmt.Sprintf("'%s'", c.args[rand.Intn(len(c.args))])
	case ColumnTypeDate, ColumnTypeTime, ColumnTypeDatetime, ColumnTypeTimestamp:
		return RandDateTime()
	default:
		return "invalid data type"
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RandDateTime() string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2037, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("01-02-2006")
}
