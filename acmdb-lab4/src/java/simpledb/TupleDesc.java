package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// import jdk.internal.util.xml.impl.Pair;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * TDItem: TupleDesc Item
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;
        
        /**
         * The buffer of the hash code
         */
        private int hashCodeBuffer = -1;
        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
            // fieldType.equals(t);
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o){
            if (this == o) return true;
            if (o==null || o.getClass()!= this.getClass()) return false;
            TDItem other = (TDItem)o;
            return (fieldName.equals(other.fieldName))&&(fieldType.equals(other.fieldType));
        }
        
        @Override
        public int hashCode(){
            if (hashCodeBuffer!=-1) return hashCodeBuffer;
            Vector<Integer> has = new Vector<>(2);
            has.add(fieldName.hashCode());
            has.add(fieldType.hashCode());
            hashCodeBuffer = has.hashCode();
            return hashCodeBuffer;
        }

    }
    /**
     * Variables
     */
    private static final long serialVersionUID = 1L;
    private Vector<TDItem> tupleDescItems;
    private int GetSize = 0;
    private ConcurrentHashMap<String,Integer> FieldNameToIndexMap;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tupleDescItems.iterator();
    }

    


    /**
     * Update the FieldNameToIndexMap
     * 
     */
    public boolean updateInfo(){
        int gsize = 0;
        FieldNameToIndexMap = new ConcurrentHashMap<>(4);
        for (int i=0;i<tupleDescItems.size();++i){
            if (tupleDescItems.get(i).fieldName!=null && 
                !FieldNameToIndexMap.containsKey(tupleDescItems.get(i).fieldName))
            FieldNameToIndexMap.put(tupleDescItems.get(i).fieldName, i);
            gsize += tupleDescItems.get(i).fieldType.getLen();
        }
        GetSize = gsize;
        return true;
    }


    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tupleDescItems = new Vector<>(typeAr.length);
        for (int i=0;i<typeAr.length;++i){
            tupleDescItems.add(new TDItem(typeAr[i],fieldAr[i]));
        }
        updateInfo();
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tupleDescItems = new Vector<>(typeAr.length);
        for (int i=0;i<typeAr.length;++i){
            tupleDescItems.add(new TDItem(typeAr[i],""));
        }
        updateInfo();
    }

    /**
     * Empty Constructor. Nothing here. 
     */
    public TupleDesc(int i) {
        // some code goes here
        if (i==0) tupleDescItems = new Vector<>();
        else tupleDescItems = new Vector<>(i);
        updateInfo();
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleDescItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i<0 || i >= numFields()) 
            throw new NoSuchElementException("index="+i+";size="+numFields());
        return tupleDescItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i<0 || i >= numFields()) 
            throw new NoSuchElementException("index="+i+";size="+numFields());
        return tupleDescItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (FieldNameToIndexMap==null) FieldNameToIndexMap = new ConcurrentHashMap<>();
        if (name==null){ throw new NoSuchElementException(); }
        Integer ans = FieldNameToIndexMap.get(name);
        if (ans==null){ throw new NoSuchElementException(); }
        return ans;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        if (GetSize==0) updateInfo();
        return GetSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc ans = new TupleDesc(td1.tupleDescItems.size()+td2.tupleDescItems.size());
        ans.tupleDescItems.addAll(td1.tupleDescItems);
        ans.tupleDescItems.addAll(td2.tupleDescItems);
        ans.updateInfo();
        return ans;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (o==null || o.getClass()!= this.getClass()) return false;
        TupleDesc other = (TupleDesc)o;
        return this.tupleDescItems.equals(other.tupleDescItems);
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.tupleDescItems.hashCode();
        // throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     * 
     * Jiasen Note: I use the Vector toString Function
     */
    public String toString() {
        // some code goes heres
        return tupleDescItems.toString();
        // return "";
    }
    /**
     * 
     * @return the length of the tupleDesc, 
     * <p>
     * How many items are in the tupleDesc
     */
    public int getLength(){
        return this.tupleDescItems.size();
    }
}
