package sqlgen

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

func NewGenerator(state *State) func() string {
	rand.Seed(time.Now().UnixNano())
	GenPlugins = append(GenPlugins, &ScopeListener{state: state})
	postListener := &PostListener{callbacks: map[string]func(){}}
	GenPlugins = append(GenPlugins, postListener)
	retFn := func() string {
		res := evaluateFn(start)
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
			return ""
		}
	}

	start = NewFn("start", func() Fn {
		if state.IsInitializing() {
			return initStart
		}
		return Or(
			//switchSysVars,
			If(len(state.tables) < state.ctrl.MaxTableNum,
				createTable,
			).SetW(3),
			If(len(state.tables) > 0,
				Or(
					dmlStmt.SetW(4),
					ddlStmt.SetW(1),
				),
			).SetW(5),
		)
	})

	initStart = NewFn("initStart", func() Fn {
		return Or(
			If(len(state.tables) < state.ctrl.InitTableCount,
				createTable,
			).SetW(3),
			If(len(state.tables) > 0,
				insertInto,
			),
		)
	})

	dmlStmt = NewFn("dmlStmt", func() Fn {
		return Or(
			query,
			commonDelete,
			commonInsert,
			commonUpdate,
		)
	})

	ddlStmt = NewFn("ddlStmt", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		return Or(
			addColumn,
			addIndex,
			If(len(tbl.columns) > 1 && tbl.HasColumnUncoveredByIndex(),
				dropColumn,
			),
			If(len(tbl.indices) > 0,
				dropIndex,
			),
		)
	})

	switchSysVars = NewFn("switchSysVars", func() Fn {
		return Or(
			Str("set @@global.tidb_row_format_version = 2"),
			Str("set @@global.tidb_row_format_version = 1"),
			Str("set @@tidb_enable_clustered_index = 0"),
			Str("set @@tidb_enable_clustered_index = 1"),
		)
	})

	createTable = NewFn("createTable", func() Fn {
		tblName := fmt.Sprintf("tbl_%d", state.AllocGlobalID(ScopeKeyTableUniqID))
		tbl := GenNewTable(tblName)
		state.AppendTable(tbl)
		postListener.Register("createTable", tbl.ReorderColumns)
		definitions = NewFn("definitions", func() Fn {
			colDefs = NewFn("colDefs", func() Fn {
				if state.IsInitializing() {
					return Repeat(colDef, state.ctrl.InitColCount, Str(","))
				}
				return Or(
					colDef,
					And(colDef, Str(","), colDefs).SetW(2),
				)
			})
			colDef = NewFn("colDef", func() Fn {
				col := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID))
				tbl.AppendColumn(col)
				return And(Str(col.name), Str(PrintColumnType(col)))
			})
			idxDefs = NewFn("idxDefs", func() Fn {
				return Or(
					idxDef,
					And(idxDef, Str(","), idxDefs).SetW(2),
				)
			})
			idxDef = NewFn("idxDef", func() Fn {
				idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
				tbl.AppendIndex(idx)
				return And(
					Str(PrintIndexType(idx)),
					Str("key"),
					Str(idx.name),
					Str("("),
					Str(PrintIndexColumnNames(idx)),
					Str(")"),
				)
			})
			return Or(
				And(colDefs, Str(","), idxDefs).SetW(4),
				colDefs,
			)
		})

		return And(
			Str("create table"),
			Str(tblName),
			Str("("),
			definitions,
			Str(")"),
		)
	})

	insertInto = NewFn("insertInto", func() Fn {
		tbl := state.GetFirstNonFullTable()
		vals := tbl.GenRandValues(tbl.columns)
		tbl.AppendRow(vals)
		return And(
			Str("insert into"),
			Str(tbl.name),
			Str("values"),
			Str("("),
			Str(PrintRandValues(vals)),
			Str(")"),
		)
	})

	query = NewFn("query", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		cols := tbl.GetRandColumns()
		commonSelect = NewFn("commonSelect", func() Fn {
			return And(Str("select"),
				Str(PrintColumnNamesWithoutPar(cols, "*")),
				Str("from"),
				Str(tbl.name),
				Str("where"),
				predicate,
			)
		})
		forUpdateOpt = NewFn("forUpdateOpt", func() Fn {
			return Opt(Str("for update"))
		})
		union = NewFn("union", func() Fn {
			return Or(
				Str("union"),
				Str("union all"),
			)
		})
		aggSelect = NewFn("aggSelect", func() Fn {
			intCol := tbl.GetRandIntColumn()
			if intCol == nil {
				return And(
					Str("select count(*) from"),
					Str(tbl.name),
					Str("where"),
					predicate,
				)
			}
			return Or(
				And(
					Str("select count(*) from"),
					Str(tbl.name),
					Str("where"),
					predicate,
				),
				And(
					Str("select sum("),
					Str(intCol.name),
					Str(")"),
					Str("from"),
					Str(tbl.name),
					Str("where"),
					predicate,
				),
			)
		})

		return Or(
			And(commonSelect, forUpdateOpt),
			And(
				Str("("), commonSelect, forUpdateOpt, Str(")"),
				union,
				Str("("), commonSelect, forUpdateOpt, Str(")"),
			),
			And(aggSelect, forUpdateOpt),
			And(
				Str("("), aggSelect, forUpdateOpt, Str(")"),
				union,
				Str("("), aggSelect, forUpdateOpt, Str(")"),
			),
		)
	})

	commonInsert = NewFn("commonInsert", func() Fn {
		tbl := state.GetRandTable()
		var cols []*Column
		if state.ctrl.StrictTransTable {
			cols = tbl.GetRandColumnsIncludedDefaultValue()
		} else {
			cols = tbl.GetRandColumns()
		}
		insertOrReplace := "insert"
		if rand.Intn(3) == 0 {
			insertOrReplace = "replace"
		}

		onDuplicateUpdate = NewFn("onDuplicateUpdate", func() Fn {
			return Or(
				Empty().SetW(3),
				And(
					Str("on duplicate key update"),
					Or(
						onDupAssignment.SetW(4),
						And(onDupAssignment, Str(","), onDupAssignment),
					),
				),
			)
		})

		onDupAssignment = NewFn("onDupAssignment", func() Fn {
			randCol := tbl.GetRandColumn()
			return Or(
				Strs(randCol.name, "=", randCol.RandomValue()),
				Strs(randCol.name, "=", "values(", randCol.name, ")"),
			)
		})

		multipleRowVals = NewFn("multipleRowVals", func() Fn {
			vals := tbl.GenRandValues(cols)
			return Or(
				Strs("(", PrintRandValues(vals), ")").SetW(3),
				And(Strs("(", PrintRandValues(vals), ")"), Str(","), multipleRowVals),
			)
		})

		return Or(
			And(
				Str(insertOrReplace),
				Str("into"),
				Str(tbl.name),
				Str(PrintColumnNamesWithPar(cols, "")),
				Str("values"),
				multipleRowVals,
				OptIf(insertOrReplace == "insert", onDuplicateUpdate),
			),
		)
	})

	commonUpdate = NewFn("commonUpdate", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		orderByCols := tbl.GetRandColumns()

		updateAssignment = NewFn("updateAssignment", func() Fn {
			randCol := tbl.GetRandColumn()
			return Or(
				Strs(randCol.name, "=", randCol.RandomValue()),
			)
		})

		return And(
			Str("update"),
			Str(tbl.name),
			Str("set"),
			updateAssignment,
			Str("where"),
			predicates,
			OptIf(len(orderByCols) > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(orderByCols, "")),
					maybeLimit,
				),
			),
		)
	})

	commonDelete = NewFn("commonDelete", func() Fn {
		tbl := state.GetRandTable()
		col := tbl.GetRandColumn()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))

		multipleRowVal = NewFn("multipleRowVal", func() Fn {
			return Or(
				Str(col.RandomValue()).SetW(3),
				And(Str(col.RandomValue()), Str(","), multipleRowVal),
			)
		})

		return And(
			Str("delete from"),
			Str(tbl.name),
			Str("where"),
			Or(
				And(predicates, maybeLimit),
				And(Str(col.name), Str("in"), Str("("), multipleRowVal, Str(")"), maybeLimit),
				And(Str(col.name), Str("is null"), maybeLimit),
			),
		)
	})

	predicates = NewFn("predicates", func() Fn {
		return Or(
			predicate.SetW(3),
			And(predicate, Or(Str("and"), Str("or")), predicates),
		)
	})

	predicate = NewFn("predicate", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		randCol := tbl.GetRandColumn()
		randColVals = NewFn("randColVals", func() Fn {
			var randVal string
			if rand.Intn(3) == 0 || len(tbl.values) == 0 {
				randVal = randCol.RandomValue()
			} else {
				randVal = tbl.GetRandRowVal(randCol)
			}
			return Or(
				Str(randVal),
				And(Str(randVal), Str(","), randColVals).SetW(3),
			)
		})
		return Or(
			Strs(randCol.name, "=", randCol.RandomValue()),
			And(Str(randCol.name), Str("in"), Str("("), randColVals, Str(")")),
		)
	})

	maybeLimit = NewFn("maybeLimit", func() Fn {
		return Or(
			Empty().SetW(3),
			Strs("limit", RandomNum(1, 10)),
		)
	})

	addIndex = NewFn("addIndex", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
		tbl.AppendIndex(idx)

		return Strs(
			"alter table", tbl.name,
			"add index", idx.name,
			"(", PrintIndexColumnNames(idx), ")",
		)
	})

	dropIndex = NewFn("dropIndex", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		idx := tbl.GetRandomIndex()
		tbl.RemoveIndex(idx)
		return Strs(
			"alter table", tbl.name,
			"drop index", idx.name,
		)
	})

	addColumn = NewFn("addColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID))
		tbl.AppendColumn(col)
		return Strs(
			"alter table", tbl.name,
			"add column", col.name, PrintColumnType(col),
		)
	})

	dropColumn = NewFn("dropColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := tbl.GetRandColumnWithIndexUncovered()
		tbl.RemoveColumn(col)
		return Strs(
			"alter table", tbl.name,
			"drop column", col.name,
		)
	})

	return retFn
}
