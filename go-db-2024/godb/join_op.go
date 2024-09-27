package godb

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// NewJoin Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	if leftField == nil || rightField == nil {
		return nil, GoDBError{TypeMismatchError, "leftField and rightField must be non-nil"}
	}

	return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
}

// Descriptor Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (joinOp *EqualityJoin) Descriptor() *TupleDesc {
	left := *joinOp.left
	right := *joinOp.right
	return left.Descriptor().merge(right.Descriptor())
}

// Iterator Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	left := *joinOp.left
	right := *joinOp.right

	leftIter, err := left.Iterator(tid)
	if err != nil {
		DPrintf("EqualityJoin Iterator get left iterator err: %v", err)
		return
	}

	var (
		reset        = true
		leftScanEnd  bool
		joinBufMap   map[any][]*Tuple
		rightIter    func() (*Tuple, error)
		validTupleCh chan *Tuple
	)
	iterFunc = func() (reply *Tuple, err error) {
		if len(validTupleCh) > 0 {
			// has valid reply
			return <-validTupleCh, nil
		}

		var (
			rightTuple  *Tuple
			rightTmpVal DBValue
			matchTuples []*Tuple
		)
		for {
			if reset {
				if leftScanEnd {
					return
				}

				joinBufMap, err = joinOp.fillJoinBufMap(leftIter, &leftScanEnd)
				if err != nil {
					return
				}

				rightIter, err = right.Iterator(tid)
				if err != nil {
					DPrintf("EqualityJoin Iterator get right iterator err: %v", err)
					return
				}
				reset = false
			}

			for {
				rightTuple, err = rightIter()
				if err != nil {
					DPrintf("EqualityJoin rightIter() err: %v", err)
					return
				}
				if rightTuple == nil {
					break
				}

				rightTmpVal, err = joinOp.rightField.EvalExpr(rightTuple)
				if err != nil {
					DPrintf("EqualityJoin leftField EvalExpr err: %v", err)
					return
				}

				matchTuples = joinBufMap[rightTmpVal]
				if len(matchTuples) == 0 {
					continue
				}

				validTupleCh = make(chan *Tuple, len(matchTuples))
				for _, tuple := range matchTuples {
					validTupleCh <- joinTuples(tuple, rightTuple)
				}
				reply = <-validTupleCh
				return
			}

			reset = true
		}
	}

	return
}

// hash join:
// Choose the bigger or has index table to be-driven table.
// Fill the driver table fill into memory hash table(cap most maxBufferSize).
// Iterate through the be-driven table and check each tuple in memory hash table.
// If the be-driven table has been iter end, re fill the hash table by iter driver table.
// Return until the driver table has been iter end.
func (joinOp *EqualityJoin) fillJoinBufMap(leftIter func() (*Tuple, error), leftScanEnd *bool) (joinBufMap map[any][]*Tuple, err error) {
	var (
		tmpTuple *Tuple
		tmpVal   DBValue
	)
	joinBufMap = make(map[any][]*Tuple, joinOp.maxBufferSize)
	for i := 0; i < joinOp.maxBufferSize; i++ {
		tmpTuple, err = leftIter()
		if err != nil {
			DPrintf("EqualityJoin leftIter() err: %v", err)
			return
		}
		if tmpTuple == nil {
			DPrintf("EqualityJoin leftIter tuple first nil")
			*leftScanEnd = true
			break
		}

		tmpVal, err = joinOp.leftField.EvalExpr(tmpTuple)
		if err != nil {
			DPrintf("EqualityJoin leftField EvalExpr err: %v", err)
			return
		}

		joinBufMap[tmpVal] = append(joinBufMap[tmpVal], tmpTuple)
	}

	return
}

func buildTupleTable(tuple *Tuple, exp Expr) {
	expType := exp.GetExprType()
	for index := range tuple.Desc.Fields {
		tuple.Desc.Fields[index].TableQualifier = expType.TableQualifier
	}
}
