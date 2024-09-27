package godb

// AggState interface for an aggregation state
type AggState interface {
	// Init Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Copy Makes an copy of the aggregation state.
	Copy() AggState

	// AddTuple Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Finalize Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// GetTupleDesc Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// CountAggState Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// SumAggState Implements the aggregation state for SUM
type SumAggState struct {
	alias string
	expr  Expr
	sum   int64
}

func (a *SumAggState) Copy() AggState {
	return &SumAggState{a.alias, a.expr, a.sum}
}

func intAggGetter(v DBValue) any {
	// TODO: some code goes here
	return nil // replace me
}

func stringAggGetter(v DBValue) any {
	// TODO: some code goes here
	return nil // replace me
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.sum = 0
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	tmpVal, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	val, ok := tmpVal.(IntField)
	if !ok {
		return
	}

	a.sum += val.Value
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
}

func (a *SumAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	return &Tuple{*td, []DBValue{IntField{a.sum}}, nil}
}

// AvgAggState Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	alias string
	expr  Expr
	sum   int64
	count int
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{a.alias, a.expr, a.sum, a.count}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.sum = 0
	a.count = 0
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	tmpVal, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	val, ok := tmpVal.(IntField)
	if !ok {
		return
	}

	a.sum += val.Value
	a.count++
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
}

func (a *AvgAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	return &Tuple{*td, []DBValue{IntField{a.sum / int64(a.count)}}, nil}
}

// MaxAggState Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	alias string
	expr  Expr
	max   DBValue
}

func (a *MaxAggState) Copy() AggState {
	return &MaxAggState{a.alias, a.expr, a.max}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.max = nil
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	tmpVal, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	if a.max == nil {
		a.max = tmpVal
		return
	}

	if tmpVal.EvalPred(a.max, OpGt) {
		a.max = tmpVal
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
}

func (a *MaxAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	return &Tuple{*td, []DBValue{a.max}, nil}
}

// MinAggState Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	alias string
	expr  Expr
	min   DBValue
}

func (a *MinAggState) Copy() AggState {
	return &MinAggState{a.alias, a.expr, a.min}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.min = nil
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	tmpVal, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	if a.min == nil {
		a.min = tmpVal
		return
	}

	if tmpVal.EvalPred(a.min, OpLt) {
		a.min = tmpVal
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
}

func (a *MinAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	return &Tuple{*td, []DBValue{a.min}, nil}
}
