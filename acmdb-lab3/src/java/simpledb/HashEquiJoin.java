package simpledb;

import java.util.*;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1,child2;
    private Tuple tup1,tup2;
    private TupleDesc tupleDesc = null;
    private HashMap <Field,Vector<Tuple>> tupHash1;
    private Iterator<Tuple> iter1;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.tupleDesc = null;
        this.tupHash1 = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        if (tupleDesc==null){
            tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        }
        return tupleDesc;
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }
    
    public void buildHashMap()throws DbException, NoSuchElementException,
    TransactionAbortedException{
        // System.out.println("build");
        tupHash1 = new HashMap<>();
        while (child1.hasNext()){
            Tuple tup1 = child1.next();
            Field f1 = tup1.getField(this.p.getField1());
            if (!tupHash1.containsKey(f1)){
                tupHash1.put(f1,new Vector<>(1));
            }
            Vector<Tuple> list = tupHash1.get(f1);
            list.add(tup1);
            // System.out.println(tup1+","+f1);
        }
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        if (child1.hasNext()&&child2.hasNext()){
            iter1 = null;
            if (tupHash1==null){
                buildHashMap();
            }
        } else {
            tup1 = null;
            tup2 = null;
        }
        super.open();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        // tupHash1 = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        if (child1.hasNext()&&child2.hasNext()){
            iter1 = null;
            if (tupHash1==null){
                buildHashMap();
            }
        } else {
            tup1 = null;
            tup2 = null;
        }
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Update the tup1 and tup2
     * <p>
     * The update next does not care about 
     * whether it is okay to Predicate
     * @throws TransactionAbortedException
     * @throws DbException
     */
    private void fetchSimpleNext()throws TransactionAbortedException, DbException{
        if (child2.hasNext()){
            tup2 = child2.next();
        } else if (child1.hasNext()){
            tup1 = child1.next();
            child2.rewind();
            tup2 = child2.next();
        } else{
            tup1 = null;
            tup2 = null;
        }
    }
    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (iter1==null||(!iter1.hasNext())){
            if (!this.child2.hasNext()) return null;
            tup2 = this.child2.next();
            Vector<Tuple> tupVector = this.tupHash1.get(tup2.getField(p.getField2()));
            if (tupVector == null) continue;
            iter1 = tupVector.iterator();
        }
        tup1 = iter1.next();
        // System.out.println("run");
        
        TupleDesc TD = this.getTupleDesc();
        Tuple ans = new Tuple(TD);
        int len1 = tup1.getTupleDesc().getLength();
        int len2 = tup2.getTupleDesc().getLength();
        for (int i=0;i<len1;++i){
            ans.setField(i, tup1.getField(i));
        }
        for (int i=0;i<len2;++i){
            ans.setField(i+len1, tup2.getField(i));
        }
        // System.out.println(ans+" , "+tup1+" + "+tup2);
        return ans;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator [] ans = new DbIterator [2];
        ans[0] = child1;
        ans[1] = child2;
        return ans;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
        tupleDesc = null;
        tupHash1 = null;
    }
    
}
