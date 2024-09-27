package godb

import (
	"sort"
)

type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
}

// NewOrderBy Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (order *OrderBy, err error) {
	if len(orderByFields) != len(ascending) {
		return nil, GoDBError{IllegalOperationError, "args invalid"}
	}

	order = &OrderBy{
		orderBy:   orderByFields,
		child:     child,
		ascending: ascending,
	}
	return

}

// Descriptor Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Iterator Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return
	}

	var tup *Tuple
	var allTuples []*Tuple
	for {
		tup, err = childIter()
		if err != nil {
			return
		}
		if tup == nil {
			break
		}

		allTuples = append(allTuples, tup)
	}

	sort.Sort(sortTuples{allTuples, o.orderBy, o.ascending})

	var index int
	iterFunc = func() (reply *Tuple, err error) {
		if index >= len(allTuples) {
			return
		}

		reply = allTuples[index]
		index++
		return
	}
	return
}

type sortTuples struct {
	allTuples []*Tuple
	orderBy   []Expr
	ascending []bool
}

func (s sortTuples) Len() int {
	return len(s.allTuples)
}

func (s sortTuples) Less(i, j int) bool {
	iTup := s.allTuples[i]
	jTup := s.allTuples[j]
	for index, expr := range s.orderBy {
		iVal, err := expr.EvalExpr(iTup)
		if err != nil {
			return false
		}

		jVal, err := expr.EvalExpr(jTup)
		if err != nil {
			return false
		}

		if iVal.EvalPred(jVal, OpEq) {
			continue
		}

		ascend := s.ascending[index]
		less := iVal.EvalPred(jVal, OpLt)
		if ascend && less || !ascend && !less {
			return true
		} else {
			return false
		}
	}

	return false
}

func (s sortTuples) Swap(i, j int) {
	s.allTuples[i], s.allTuples[j] = s.allTuples[j], s.allTuples[i]
}
