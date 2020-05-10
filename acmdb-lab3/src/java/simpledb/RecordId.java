package simpledb;

import java.io.Serializable;
import java.util.Vector;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private PageId PageNum = null;
    private int TupleNum = -1;
    // private Vector<Integer> hashCoder = null;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        PageNum = pid;
        TupleNum = tupleno;
        // hashCoder = new Vector<>(2);
        // hashCoder.add(pid.hashCode());
        // hashCoder.add(tupleno);
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return TupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return PageNum;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (this==o) return true;
        if (o==null || this.getClass()!=o.getClass()) return false;
        RecordId other = (RecordId) o;
        return this.getPageId().equals(other.getPageId()) 
            && this.tupleno() == other.tupleno();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        // return hashCoder.hashCode();
        return PageNum.hashCode()*((1<<15)+97)+TupleNum*17;
    }

}
