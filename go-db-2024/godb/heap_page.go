package godb

import (
	"bytes"
	"encoding/binary"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// meta data
	pageNo int
	dirty  bool
	desc   *TupleDesc
	file   *HeapFile

	// page data
	slotCount int32
	slotUsed  int32
	tuples    []*Tuple
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (page *heapPage, err error) {
	var perTupleSize int32
	for _, field := range desc.Fields {
		switch field.Ftype {
		case IntType:
			perTupleSize += 8
		case StringType:
			perTupleSize += int32(StringLength)
		default:
			DPrintf("newHeapPage invalid field type: %d", field.Ftype)
			return nil, GoDBError{IncompatibleTypesError, "unknown field type"}
		}
	}

	remPageSize := int32(PageSize - 8)
	page = &heapPage{
		pageNo:    pageNo,
		slotCount: remPageSize / perTupleSize,
		slotUsed:  0,
		desc:      desc,
		file:      f,
	}
	page.tuples = make([]*Tuple, page.slotCount)
	return
}

func (h *heapPage) getNumSlots() int {
	return int(h.slotCount)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (id recordID, err error) {
	for index, tuple := range h.tuples {
		if tuple != nil {
			continue
		}

		id = getRecordID(h.pageNo, index)
		t.Rid = id
		h.tuples[index] = &Tuple{
			Desc:   *h.desc,
			Fields: t.Fields,
			Rid:    id,
		}
		h.slotUsed++
		h.dirty = true
		break
	}

	if id == nil {
		DPrintf("heapPage page:%d insertTuple no free slot", h.pageNo)
		err = GoDBError{PageFullError, "can not find free slot"}
	}
	return
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	_, slot := splitRecordID(rid)
	if slot < 0 || slot >= len(h.tuples) {
		DPrintf("heapPage page:%d deleteTuple rid:%s invalid", h.pageNo, rid)
		return GoDBError{TupleNotFoundError, "invalid record id"}
	}

	if h.tuples[slot] == nil {
		DPrintf("heapPage page:%d deleteTuple slot:%d already nil", h.pageNo, slot)
		return GoDBError{TupleNotFoundError, "invalid record id"}
	}

	h.tuples[slot] = nil
	h.slotUsed--
	h.dirty = true
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	// TODO tid
	h.dirty = dirty
	return
}

// Page method - return the corresponding HeapFile
// for this page.
func (h *heapPage) getFile() DBFile {
	return h.file
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (buf *bytes.Buffer, err error) {
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, h.slotCount)
	if err != nil {
		DPrintf("heapPage page:%d toBuffer Write slot count err:%v", h.pageNo, err)
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, h.slotUsed)
	if err != nil {
		DPrintf("heapPage page:%d toBuffer Write slot used err:%v", h.pageNo, err)
		return nil, err
	}

	for _, tuple := range h.tuples {
		if tuple == nil {
			continue
		}

		err = tuple.writeTo(buf)
		if err != nil {
			DPrintf("heapPage page:%d toBuffer Write tuple err:%v", h.pageNo, err)
			return nil, err
		}
	}

	if buf.Len() < PageSize {
		// padding the page to PageSize
		padding := make([]byte, PageSize-buf.Len())
		_, err = buf.Write(padding)
		if err != nil {
			return nil, err
		}
	}
	return
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) (err error) {
	err = binary.Read(buf, binary.LittleEndian, &h.slotCount)
	if err != nil {
		DPrintf("heapPage page:%d initFromBuffer Read slot count err:%v", h.pageNo, err)
		return
	}
	h.tuples = make([]*Tuple, h.slotCount)

	err = binary.Read(buf, binary.LittleEndian, &h.slotUsed)
	if err != nil {
		DPrintf("heapPage page:%d initFromBuffer Read slot used err:%v", h.pageNo, err)
		return
	}

	var tuple *Tuple
	for i := 0; i < int(h.slotUsed); i++ {
		tuple, err = readTupleFrom(buf, h.desc)
		if err != nil {
			DPrintf("heapPage page:%d initFromBuffer readTupleFrom err:%v", h.pageNo, err)
			return err
		}

		tuple.Desc = *h.desc
		tuple.Rid = getRecordID(h.pageNo, i)
		h.tuples[i] = tuple
	}
	return
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (h *heapPage) tupleIter() func() (*Tuple, error) {
	var iter int
	return func() (reply *Tuple, err error) {
		if h.slotUsed == 0 {
			return
		}
		for {
			if iter >= len(h.tuples) {
				return
			}

			reply = h.tuples[iter]
			iter++
			if reply == nil {
				continue
			}

			return
		}
	}
}
