package simpledb;
import java.util.NoSuchElementException;
// import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * disposed
 * Not Used! 
 */
public class HeapFileIterator implements DbFileIterator {
    /**
     * Elements in HeapFileIterator
     */
    private HeapFile heapFile;
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() throws DbException, TransactionAbortedException{


    }

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    public boolean hasNext()throws DbException, TransactionAbortedException{
        // AssertionError(1==2);
        throw new AssertionError();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()throws DbException, TransactionAbortedException, NoSuchElementException{
        throw new AssertionError();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException{

    }

    /**
     * Closes the iterator.
     */
    public void close(){

    }

}