package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	limit     int64
}

// NewLimitOp Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	limitVal, err := lim.EvalExpr(&Tuple{})
	if err != nil {
		return nil
	}

	limit, ok := limitVal.(IntField)
	if !ok {
		return nil
	}

	return &LimitOp{child, lim, limit.Value}
}

// Descriptor Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Iterator Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return
	}

	var count int64
	iterFunc = func() (reply *Tuple, err error) {
		var tuple *Tuple
		for {
			tuple, err = childIter()
			if err != nil {
				return
			}
			if tuple == nil {
				return
			}

			if count >= l.limit {
				return
			}

			reply = tuple
			count++
			return
		}
	}
	return
}
