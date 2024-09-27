package godb

type InsertOp struct {
	insertFile DBFile
	child      Operator
}

// NewInsertOp Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{
		insertFile: insertFile,
		child:      child,
	}
}

// Descriptor The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Iterator Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constructor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (i *InsertOp) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	iterFunc = func() (reply *Tuple, err error) {
		var tuple *Tuple
		var insert int64
		childIter, err := i.child.Iterator(tid)
		for {
			tuple, err = childIter()
			if err != nil {
				return
			}
			if tuple == nil {
				break
			}

			err = i.insertFile.insertTuple(tuple, tid)
			if err != nil {
				return
			}
			insert++
		}

		reply = &Tuple{
			Desc:   *i.Descriptor(),
			Fields: []DBValue{IntField{insert}},
		}
		return
	}
	return
}
