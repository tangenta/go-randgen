package sqlgen

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

func NewGenerator(state *State) func() string {
	rand.Seed(time.Now().UnixNano())
	retFn := func() string {
		res := start.F()
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
		return Or(
			//switchSysVars,
			If(len(state.tables) < state.ctrl.MaxTableNum,
				createTable,
			).SetW(2),
			If(len(state.tables) > 0,
				Or(
					insertInto,
					query,
					commonDelete,
					commonInsert,
					commonUpdate,
				),
			).SetW(3),
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
		definitions = NewFn("definitions", func() Fn {
			colDefs = NewFn("colDefs", func() Fn {
				return Or(
					colDef,
					And(colDef, Str(","), colDefs).SetW(2),
				)
			})
			colDef = NewFn("colDef", func() Fn {
				colName := fmt.Sprintf("col_%d", state.AllocGlobalID(ScopeKeyColumnUniqID))
				col := GenNewColumn(colName)
				tbl.AppendColumn(col)
				return And(Str(colName), Str(PrintColumnType(col)))
			})
			idxDefs = NewFn("idxDefs", func() Fn {
				return Or(
					idxDef,
					And(idxDef, Str(","), idxDefs).SetW(2),
				)
			})
			idxDef = NewFn("idxDef", func() Fn {
				idxName := fmt.Sprintf("idx_%d", state.AllocGlobalID(ScopeKeyIndexUniqID))
				idx := GenNewIndex(idxName, tbl)
				tbl.AppendIndex(idx)
				return And(
					Str(PrintIndexType(idx)),
					Str("key"),
					Str(idxName),
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
		tbl := state.GetRandTable()
		cols := tbl.GetRandColumns()
		vals := tbl.GenRandValues(cols)
		tbl.AppendRow(vals)
		return And(
			Str("insert into"),
			Str(tbl.name),
			Str(PrintColumnNamesWithPar(cols, "")),
			Str("values"),
			Str("("),
			Str(PrintRandValues(vals)),
			Str(")"),
		)
	})

	query = NewFn("query", func() Fn {
		tbl := state.GetRandTable()
		state.CreateScopeAndStore(ScopeKeyCurrentTable, NewScopeObj(tbl))
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
		).SetAfterCall(state.DestroyScope)
	})

	commonInsert = NewFn("commonInsert", func() Fn {
		tbl := state.GetRandTable()
		cols := tbl.GetRandColumns()

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
				Or(Str("insert"), Str("replace")),
				Str("into"),
				Str(tbl.name),
				Str(PrintColumnNamesWithPar(cols, "")),
				Str("values"),
				multipleRowVals,
				onDuplicateUpdate,
			),
		)
	})

	commonUpdate = NewFn("commonUpdate", func() Fn {
		tbl := state.GetRandTable()
		state.CreateScopeAndStore(ScopeKeyCurrentTable, NewScopeObj(tbl))
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
			OptIf(len(orderByCols) != 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithPar(orderByCols, "")),
					maybeLimit,
				),
			),
		).SetAfterCall(state.DestroyScope)
	})

	commonDelete = NewFn("commonDelete", func() Fn {
		tbl := state.GetRandTable()
		col := tbl.GetRandColumn()
		state.CreateScopeAndStore(ScopeKeyCurrentTable, NewScopeObj(tbl))

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
		).SetAfterCall(state.DestroyScope)
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
			return Or(
				Str(randCol.RandomValue()),
				And(Str(randCol.RandomValue()), Str(","), randColVals),
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

	return retFn
}
