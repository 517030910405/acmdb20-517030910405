package simpledb;

import java.io.IOException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private TupleDesc TD;
    private DbFile f;
    private boolean visit;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.f = Database.getCatalog().getDatabaseFile(tableId);
        if (!f.getTupleDesc().equals(child.getTupleDesc())) {
            System.err.println(f.getTupleDesc()+","+child.getTupleDesc());
            throw new DbException("Different TD （づ￣3￣）づ╭❤～");
        }
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (visit) return null;
        visit = true;
        // if (!child.hasNext()) return null;
        int cnt = 0;
        while (child.hasNext()){
            try{
                // t.schema;
                Tuple tup = child.next();
                tup.schema = f.getTupleDesc();
                Database.getBufferPool().insertTuple(t, tableId, tup);
            } catch(IOException e){
                System.out.println(e);
                throw new NotImplementedException();
            }
            ++cnt;
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
        if (!f.getTupleDesc().equals(child.getTupleDesc())) 
          throw new NotImplementedException();
    }
}
