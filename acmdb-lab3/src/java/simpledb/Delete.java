package simpledb;

import java.io.IOException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private TupleDesc TD;
    private DbFile f;
    private boolean visit;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        Type [] TDtype = new Type[1];
        TDtype[0] = Type.INT_TYPE;
        this.TD = new TupleDesc(TDtype);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        visit = false;
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        visit = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (visit) return null;
        visit = true;
        int cnt=0;
        while (child.hasNext()){
            Tuple tup = child.next();
            try{
                Database.getBufferPool().deleteTuple(t, tup);
                ++cnt;
            }
            catch(IOException e){
                System.out.println(e);
                throw new NotImplementedException();
            }
        }
        IntField intField = new IntField(cnt);
        Tuple tup = new Tuple(TD);
        tup.setField(0, intField);
        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator [] ans = new DbIterator[1];
        ans[0] = child;
        return ans;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
