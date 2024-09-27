package godb

type DeleteOp struct {
	deleteFile DBFile
	child      Operator
}

// NewDeleteOp Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{
		deleteFile: deleteFile,
		child:      child,
	}
}

// Descriptor The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (d *DeleteOp) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Iterator Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (d *DeleteOp) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	iterFunc = func() (reply *Tuple, err error) {
		var tuple *Tuple
		var del int64
		childIter, err := d.child.Iterator(tid)
		for {
			tuple, err = childIter()
			if err != nil {
				return
			}
			if tuple == nil {
				break
			}

			err = d.deleteFile.deleteTuple(tuple, tid)
			if err != nil {
				return
			}
			del++
		}

		reply = &Tuple{
			Desc:   *d.Descriptor(),
			Fields: []DBValue{IntField{del}},
		}
		return
	}
	return
}
