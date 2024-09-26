package godb

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// NewFilter Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Descriptor Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	return f.child.Descriptor()
}

// Iterator Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	childIter, err := f.child.Iterator(tid)
	if err != nil {
		DPrintf("Filter Iterator get child iterator err: %v", err)
		return
	}

	iterFunc = func() (reply *Tuple, err error) {
		var (
			tuple    *Tuple
			leftVal  DBValue
			rightVal DBValue
		)
		for {
			tuple, err = childIter()
			if err != nil {
				DPrintf("Filter Iterator childIter() err: %v", err)
				return
			}
			if tuple == nil {
				break
			}

			// compare conditions
			leftVal, err = f.left.EvalExpr(tuple)
			if err != nil {
				DPrintf("Filter left EvalExpr err: %v", err)
				return
			}

			rightVal, err = f.right.EvalExpr(tuple)
			if err != nil {
				DPrintf("Filter right EvalExpr err: %v", err)
				return
			}

			if !leftVal.EvalPred(rightVal, f.op) {
				// not match, iter the next tuple
				continue
			}

			reply = tuple
			return
		}
		return
	}
	return
}
