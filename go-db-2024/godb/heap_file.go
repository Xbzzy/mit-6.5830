package godb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	fromFile  string
	file      *os.File
	desc      *TupleDesc
	bufPool   *BufferPool
	idlePage  map[int]struct{}
	pageCount int
}

// NewHeapFile Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (heapFile *HeapFile, err error) {
	heapFile = &HeapFile{
		fromFile: fromFile,
		desc:     td,
		bufPool:  bp,
		idlePage: make(map[int]struct{}),
	}

	heapFile.pageCount = heapFile.NumPages()
	return
}

// BackingFile Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	return f.fromFile
}

// NumPages Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	fileInfo, err := os.Stat(f.fromFile)
	if err != nil {
		DPrintf("HeapFile path:%s NumPages Stat path:%s err:%v", f.fromFile, f.fromFile, err)
		return 0
	}

	num := int(fileInfo.Size() / int64(PageSize))
	if fileInfo.Size()%int64(PageSize) != 0 {
		num++
	}
	return num
}

// LoadFromCSV Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}

		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}

		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}

		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	file, err := os.OpenFile(f.fromFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		DPrintf("HeapFile path:%s readPage OpenFile path:%s err:%v", f.fromFile, f.fromFile, err)
		return nil, err
	}

	_, err = file.Seek(int64(pageNo*PageSize), io.SeekStart)
	if err != nil {
		DPrintf("HeapFile path:%s readPage Seek err:%v", f.fromFile, err)
		return nil, err
	}

	data := make([]byte, PageSize)
	_, err = file.Read(data)
	if err != nil {
		DPrintf("HeapFile path:%s readPage Read err:%v", f.fromFile, err)
		return nil, err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		DPrintf("HeapFile path:%s readPage Write err:%v", f.fromFile, err)
		return nil, err
	}

	hp := &heapPage{
		pageNo: pageNo,
		desc:   f.desc,
		file:   f,
	}
	err = hp.initFromBuffer(buf)
	if err != nil {
		DPrintf("HeapFile path:%s readPage initFromBuffer err:%v", f.fromFile, err)
		return nil, err
	}

	return hp, nil
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) (err error) {
	if len(t.Desc.Fields) != len(t.Fields) {
		return GoDBError{IllegalOperationError, "invalid tuple"}
	}
	if !f.desc.equals(&t.Desc) {
		return GoDBError{TypeMismatchError, "tuple desc not match"}
	}

	var (
		reply     Page
		tmpPage   *heapPage
		validPage *heapPage
	)
	for pageNo := range f.idlePage {
		reply, err = f.bufPool.GetPage(f, pageNo, tid, WritePerm)
		if err != nil {
			DPrintf("HeapFile path:%s insertTuple GetPage err:%v", f.fromFile, err)
			return
		}

		tmpPage = reply.(*heapPage)
		if tmpPage.slotUsed >= tmpPage.slotCount {
			delete(f.idlePage, tmpPage.pageNo)
			continue
		}

		// slot not full
		validPage = tmpPage
		break
	}

	if validPage == nil {
		DPrintf("HeapFile path:%s insertTuple valid nil, new page", f.fromFile)
		validPage, err = newHeapPage(f.desc, f.pageCount, f)
		if err != nil {
			DPrintf("HeapFile path:%s insertTuple newHeapPage err:%v", f.fromFile, err)
			return
		}

		_, err = validPage.insertTuple(t)
		if err != nil {
			DPrintf("HeapFile path:%s page insertTuple err:%v", f.fromFile, err)
			return
		}

		err = f.flushPage(validPage)
		if err != nil {
			DPrintf("HeapFile path:%s page flushPage err:%v", f.fromFile, err)
			return
		}

		if len(f.bufPool.Pages) < f.bufPool.PageNum {
			f.bufPool.Pages[f.pageKey(f.pageCount)] = validPage
		}

		f.idlePage[f.pageCount] = struct{}{}
		f.pageCount++
		return
	}

	_, err = validPage.insertTuple(t)
	if err != nil {
		DPrintf("HeapFile path:%s page insertTuple err:%v", f.fromFile, err)
		return
	}
	validPage.setDirty(tid, true)
	return
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) (err error) {
	pageNo, _ := splitRecordID(t.Rid)
	tmpPage, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	if err != nil {
		DPrintf("HeapFile path:%s deleteTuple readPage err:%v", f.fromFile, err)
		return
	}

	page := tmpPage.(*heapPage)
	err = page.deleteTuple(t.Rid)
	if err != nil {
		DPrintf("HeapFile path:%s page deleteTuple err:%v", f.fromFile, err)
		return
	}

	f.idlePage[pageNo] = struct{}{}
	return
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) (err error) {
	if f.file == nil {
		f.file, err = os.OpenFile(f.fromFile, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			DPrintf("HeapFile path:%s flushPage OpenFile err:%v", f.fromFile, err)
			return
		}
	}

	page := p.(*heapPage)
	_, err = f.file.Seek(int64(page.pageNo*PageSize), io.SeekStart)
	if err != nil {
		DPrintf("HeapFile path:%s flushPage Seek err:%v", f.fromFile, err)
		return
	}

	buf, err := page.toBuffer()
	if err != nil {
		DPrintf("HeapFile path:%s flushPage ToBuffer err:%v", f.fromFile, err)
		return
	}

	_, err = buf.WriteTo(f.file)
	if err != nil {
		DPrintf("HeapFile path:%s flushPage WriteTo err:%v", f.fromFile, err)
		return
	}

	page.dirty = false
	return
}

// Descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.desc
}

// Iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	var iterIndex int
	tupleIterMap := make(map[int]func() (*Tuple, error))
	return func() (tuple *Tuple, err error) {
		var (
			tmpPage Page
			page    *heapPage
			i       int
		)
		for i = iterIndex; i < f.pageCount; i++ {
			tmpPage, err = f.bufPool.GetPage(f, i, tid, ReadPerm)
			if err != nil {
				DPrintf("HeapFile path:%s Iterator GetPage err:%v", f.fromFile, err)
				return
			}

			page = tmpPage.(*heapPage)

			if tupleIterMap[i] == nil {
				tupleIterMap[i] = page.tupleIter()
			}

			tuple, err = tupleIterMap[i]()
			if tuple == nil {
				iterIndex++
				continue
			}

			return
		}

		return
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{
		FileName: f.fromFile,
		PageNo:   pgNo,
	}
}
