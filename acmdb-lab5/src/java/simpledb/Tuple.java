package simpledb;

import java.io.Serializable;
// import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    /**
     * Variables
     */
    private static final long serialVersionUID = 1L;
    public TupleDesc schema = null;
    private RecordId recordId = null;
    private Field[] tuple_record = null;
    
    private void tuple_record_push_n_times(int n){
        for (int i=0;i<n;++i){
            tuple_record[i] = null;
        }
    }

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tuple_record = new Field[td.numFields()];
        tuple_record_push_n_times(td.numFields());
        schema = td;
        // assert record.get(0)==null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        // tuple_record.set(i, f);
        tuple_record[i] =f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        // return tuple_record.get(i);
        return tuple_record[i];
    }

    /**
     * @return whether equals o or not <p>
     * Based on the <B><I>Values</I></B> in the Tuple
     */
    @Override
    public boolean equals(Object o){
        // boolean ans = true;
        if (o == null) return false;
        if (o.getClass()!=this.getClass()) return false;
        Tuple ot = (Tuple) o;
        if (ot.tuple_record.length!=this.tuple_record.length) return false;
        int len = tuple_record.length;
        for (int i=0;i<len;++i){
            if (!this.getField(i).equals(ot.getField(i))){
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String ans = "[";
        for (int i=0;i<tuple_record.length;++i){
            ans += tuple_record[i]+",";
        }
        return ans+"]";
        // throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new Iterator<Field>() {
            private int pos = 0;
            @Override
            public boolean hasNext() {
                // TODO Auto-generated method stub
                return pos<tuple_record.length;
            }
            @Override
            public Field next() {
                // TODO Auto-generated method stub
                return tuple_record[pos++];
            }
            @Override
            public int hashCode() {
                // TODO Auto-generated method stub
                return tuple_record.hashCode()*97+pos*17;
            }
        };
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tuple_record = new Field[td.numFields()];
        tuple_record_push_n_times(td.numFields());
        recordId = null;
        schema = td;
    }

    // public static Tuple merge(Tuple tup1,Tuple tup2){
        
    //     return null;
    // }
}
