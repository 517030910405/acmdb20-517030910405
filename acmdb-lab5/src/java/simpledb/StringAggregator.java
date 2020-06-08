package simpledb;

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Vector;
import java.util.Iterator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private AggregatorNode aNode = null;
    private HashMap<Field, AggregatorNode> aNodes = null;
    private TupleDesc TD;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
    throws IllegalArgumentException {
        // some code goes here
        if (what!=Op.COUNT) throw new IllegalArgumentException("what != COUNT by Jiasen");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (this.gbfield == Aggregator.NO_GROUPING){
            aNode = new AggregatorNode();
            Type[] TDtype = new Type[1];
            TDtype[0] = Type.INT_TYPE;
            TD = new TupleDesc(TDtype);
        } else{
            aNodes = new HashMap<>();
            Type[] TDtype = new Type[2];
            TDtype[0] = gbfieldtype;
            TDtype[1] = Type.INT_TYPE;
            TD = new TupleDesc(TDtype);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // int new_value = ((IntField)tup.getField(afield)).getValue();
        if (this.gbfield == Aggregator.NO_GROUPING){
            this.aNode.add();
        } else{
            Field f = tup.getField(gbfield);
            if (!this.aNodes.containsKey(f))aNodes.put(f, new AggregatorNode());
            aNodes.get(f).add();
        }
    }

    public TupleDesc getTupleDesc(){
        return TD;
    }
    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab3");
        return new DbIterator(){
            Iterator<Tuple> iter = null;
            Vector<Tuple> ans = null;
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                iter = ans.iterator();
            }
        
            @Override
            public void open() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                if (gbfield == Aggregator.NO_GROUPING){
                    ans = new Vector<>(1);
                    Tuple tup = new Tuple(TD);
                    Field f = aNode.getValue(what);
                    tup.setField(0, f);
                    ans.add(tup);
                } else{
                    ans = new Vector<>(2);
                    Iterator<HashMap.Entry<Field, AggregatorNode>> entIter = 
                        aNodes.entrySet().iterator();
                    while (entIter.hasNext()){
                        HashMap.Entry<Field, AggregatorNode> entry = entIter.next();
                        Tuple tup = new Tuple(TD);
                        tup.setField(0, entry.getKey());
                        tup.setField(1, entry.getValue().getValue(what));
                        ans.add(tup);
                    }
                }
                iter = ans.iterator();
            }
        
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // TODO Auto-generated method stub
                return iter.next();
            }
        
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                return iter.hasNext();
            }
        
            @Override
            public TupleDesc getTupleDesc() {
                // TODO Auto-generated method stub
                return TD;
            }
        
            @Override
            public void close() {
                // TODO Auto-generated method stub
                // Do nothing
            }
        };
    }
    public class AggregatorNode {
        public int cnt;
        public AggregatorNode(){
            cnt = 0;
        }
        public void add(){
            cnt += 1;
        }
        public boolean isAvailable(){
            return cnt>0;
        }
        public Field getValue(Op what){
            if (what.equals(Op.COUNT)){
                return new IntField(cnt);
            }
            throw new NotImplementedException();
            // return null;
        }
    }

}
