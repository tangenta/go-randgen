package gendata

import (
	"bytes"
	"fmt"
	"github.com/yuin/gopher-lua"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Tables struct {
	*options
}

var tablesTmpl = mustParse("tables", "create table {{.tname}} (\n" +
"`pk` int%s\n" +
"{{.pk_counts}}\n" +
") {{.charsets}} {{.partitions}}")

// support vars
var tablesVars = []*varWithDefault{
	{
		"rows",
		[]string{"0", "1", "2", "10", "100"},
	},
	{
		"charsets",
		[]string{"undef"},
	},
	{
		"partitions",
		[]string{"undef"},
	},
	{
		"pk_counts",
		[]string{"1"},
	},
}

// process function
var tableFuncs = map[string]func(string, *tableStmt) (string, error){
	"rows": func(text string, stmt *tableStmt) (s string, e error) {
		rows, err := strconv.Atoi(text)
		if err != nil {
			return "", err
		}

		stmt.rowNum = rows
		return "", nil
	},
	"charsets": func(text string, stmt *tableStmt) (s string, e error) {
		if text == "undef" {
			return "", nil
		}
		return fmt.Sprintf("character set %s", text), nil
	},
	"partitions": func(text string, stmt *tableStmt) (s string, e error) {
		if text == "undef" {
			return "", nil
		}
		num, err := strconv.Atoi(text)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("\npartition by hash(pk)\npartitions %d", num), nil
	},
	"pk_counts": func(text string, stmt *tableStmt) (string, error) {
		if text == "undef" {
			text = "1"
		}
		num, err := strconv.Atoi(text)
		if err != nil {
			return "", err
		}
		stmt.primaryKeyCount = num
		return "primary key(%s)", nil
	},
}

func newTables(l *lua.LState) (*Tables, error) {
	o, err := newOptions(tablesTmpl, l, "tables", tablesVars)

	if err != nil {
		return nil, err
	}

	return &Tables{o}, nil
}

func (t *Tables) gen() ([]*tableStmt, error) {
	tnamePrefix := "table"

	buf := &bytes.Buffer{}
	m := make(map[string]string)
	stmts := make([]*tableStmt, 0, t.numbers)

	err := t.traverse(func(cur []string) error {
		buf.Reset()
		buf.WriteString(tnamePrefix)
		stmt := &tableStmt{}
		for i := range cur {
			// current field name: fields[i]
			// current field value: curr[i]
			field := t.fields[i]
			buf.WriteString("_" + cur[i])
			target, err := tableFuncs[field](cur[i], stmt)
			if err != nil {
				return err
			}
			m[field] = target
		}

		tname := buf.String()

		stmt.name = tname

		m["tname"] = tname

		stmt.format = t.format(m)

		stmts = append(stmts, stmt)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return stmts, nil
}

type tableStmt struct {
	// create statement without field part
	format string
	// table name
	name string
	rowNum int
	// generate by wrapInTable
	ddl string
	// column number contained in the primary key
	primaryKeyCount int
}

func (t *tableStmt) wrapInTable(fieldStmts []string, fieldExecs []*fieldExec) {
	buf := &bytes.Buffer{}
	buf.WriteString(",\n")
	buf.WriteString(strings.Join(fieldStmts, ",\n"))
	t.ddl = fmt.Sprintf(t.format, buf.String(), randomFieldNames(t.primaryKeyCount, fieldExecs))
}

func randomFieldNames(cnt int, fieldExecs []*fieldExec) string {
	names := make([]string, len(fieldExecs))
	for i := range fieldExecs {
		names[i] = fieldExecs[i].name
	}
	shuffle(names)
	if cnt > len(names) {
		cnt = len(names)
	}
	return strings.Join(names[:cnt], ", ")
}

func shuffle(vals []string) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(vals) > 0 {
		n := len(vals)
		randIndex := r.Intn(n)
		vals[n-1], vals[randIndex] = vals[randIndex], vals[n-1]
		vals = vals[:n-1]
	}
}