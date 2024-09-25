package godb

import "os"

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (reply int, err error) {
	index, err := findFieldInTd(FieldType{Fname: sumField}, &td)
	if err != nil {
		DPrintf("computeFieldSum findFieldInTd error: %v", err)
		return
	}

	field := td.Fields[index]
	if field.Ftype != IntType {
		DPrintf("computeFieldSum field type:%d error", field.Ftype)
		return
	}

	heapFile, err := NewHeapFile("test", &td, bp)
	if err != nil {
		DPrintf("computeFieldSum NewHeapFile error: %v", err)
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		DPrintf("computeFieldSum Open error: %v", err)
		return
	}

	err = heapFile.LoadFromCSV(file, true, ",", false)
	if err != nil {
		DPrintf("computeFieldSum LoadFromCSV error: %v", err)
		return
	}

	iter, err := heapFile.Iterator(0)
	if err != nil {
		DPrintf("computeFieldSum get Iterator error: %v", err)
		return
	}

	var (
		tup    *Tuple
		tmpInt IntField
		ok     bool
	)
	for {
		tup, err = iter()
		if err != nil {
			DPrintf("computeFieldSum iter() error: %v", err)
			return
		}

		if tup == nil {
			break
		}

		tmpInt, ok = tup.Fields[index].(IntField)
		if !ok {
			DPrintf("computeFieldSum tup field invalid:%v error", tup)
			return
		}

		reply += int(tmpInt.Value)
	}
	return
}
