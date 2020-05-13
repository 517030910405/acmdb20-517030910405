package simpledb;

import java.util.Vector;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {


    private int TableID = -1;
    private int PageNum = -1;
    // private Vector<Integer> hashCoder = null;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        TableID = tableId;
        PageNum = pgNo;
        // hashCoder = new Vector<>();
        // hashCoder.add(tableId);
        // hashCoder.add(pgNo);
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return TableID;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        return PageNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        // return hashCoder.hashCode();
        return PageNum*((1<<15)+97)+TableID*((1<<5)+13);
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (o==null || getClass()!=o.getClass()) return false;
        HeapPageId other = (HeapPageId) o; 
        return (this.pageNumber() == other.pageNumber() && this.getTableId() == other.getTableId());
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

    
}
