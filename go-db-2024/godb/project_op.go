package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
}

// NewProjectOp Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (project Operator, err error) {
	if len(selectFields) != len(outputNames) {
		return nil, GoDBError{IllegalOperationError, ""}
	}

	project = &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		distinct:     distinct,
		child:        child,
	}
	return
}

// Descriptor Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() (reply *TupleDesc) {
	reply = &TupleDesc{
		Fields: make([]FieldType, 0, len(p.selectFields)),
	}

	var tmpDesc FieldType
	for index, field := range p.selectFields {
		tmpDesc = field.GetExprType()
		tmpDesc.Fname = p.outputNames[index]
		reply.Fields = append(reply.Fields, tmpDesc)
	}
	return
}

// Iterator Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (iterFunc func() (*Tuple, error), err error) {
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return
	}

	desc := *p.Descriptor()

	var allMap map[any]struct{}
	if p.distinct {
		allMap = make(map[any]struct{})
	}

	iterFunc = func() (reply *Tuple, err error) {
		var (
			tuple    *Tuple
			newTup   *Tuple
			tupleKey any
		)
		for {
			tuple, err = childIter()
			if err != nil {
				return
			}
			if tuple == nil {
				return
			}

			var tmpVal DBValue
			newTup = &Tuple{
				Desc:   desc,
				Fields: make([]DBValue, 0, len(p.selectFields)),
			}
			for _, field := range p.selectFields {
				tmpVal, err = field.EvalExpr(tuple)
				if err != nil {
					return
				}
				newTup.Fields = append(newTup.Fields, tmpVal)
			}

			if !p.distinct {
				reply = newTup
				return
			}

			tupleKey = newTup.tupleKey()
			if _, isExist := allMap[tupleKey]; isExist {
				continue
			}

			allMap[tupleKey] = struct{}{}
			reply = newTup
			return
		}

	}
	return
}
