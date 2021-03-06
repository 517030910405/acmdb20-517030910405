package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.IntegerAggregator.AggregatorNode;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int numOfBuckets = NUM_HIST_BINS;
    private IntHistogram [] Hists;
    private StringHistogram [] StrHists;
    private DbFile file;
    private int tableid;
    private int ioCostPerPage;
    private TupleDesc desc;
    private DbFileIterator iter;
    private int numOfTuples = 0; 
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.desc = file.getTupleDesc();
        int len = desc.getLength();
        Transaction t = new Transaction();
        iter = file.iterator(t.getId());
        this.Hists = new IntHistogram[len];
        this.StrHists = new StringHistogram[len];
        MinMaxNode [] nodes = new MinMaxNode[len];
        
        // First Scan for its max and min value
        for (int i=0;i<len;++i){
            nodes[i] = new MinMaxNode();
        }
        try{
            iter.open();
            numOfTuples = 0;
            while (iter.hasNext()){
                Tuple tup = iter.next();
                ++numOfTuples;
                // System.out.println(tup);
                for (int i=0;i<len;++i){
                    if (desc.getFieldType(i)==Type.INT_TYPE){
                        nodes[i].add(((IntField)tup.getField(i)).getValue());
                    } else if(desc.getFieldType(i)==Type.STRING_TYPE){
                        // No Need to get the Max or Min String of the answer
                    } else throw new NotImplementedException(); 
                }
            }
        } catch(DbException e){
            throw new NotImplementedException();
        } catch(TransactionAbortedException e){
            throw new NotImplementedException();
        }

        // Second Scan to get the buckets
        for (int i=0;i<len;++i){
            if (desc.getFieldType(i)==Type.INT_TYPE){
                Hists[i] = new IntHistogram(numOfBuckets, nodes[i].min, nodes[i].max);
                StrHists[i] = null;
            }else{
                Hists[i] = null;
                StrHists[i] = new StringHistogram(numOfBuckets);
            }
        }
        try{
            iter.rewind();
            while (iter.hasNext()){
                Tuple tup = iter.next();
                for (int i=0;i<len;++i){
                    if (desc.getFieldType(i)==Type.INT_TYPE){
                        Hists[i].addValue(((IntField)tup.getField(i)).getValue());
                    }
                }
            }
        } catch(DbException e){
            throw new NotImplementedException();
        } catch(TransactionAbortedException e){
            throw new NotImplementedException();
        }
        iter.close();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage*file.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)Math.floor(this.totalTuples()*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        // TODO: Not sure if it is needed.
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (desc.getFieldType(field)==Type.INT_TYPE){
            double ans = this.Hists[field].estimateSelectivity(op, ((IntField)constant).getValue());
            // System.out.println(((IntField)constant).getValue()+":"+ans);
            return ans;
        } else if (desc.getFieldType(field)==Type.STRING_TYPE){
            return this.StrHists[field].estimateSelectivity(op, ((StringField)constant).getValue());
        } else throw new NotImplementedException();
        // return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numOfTuples;
    }

}
