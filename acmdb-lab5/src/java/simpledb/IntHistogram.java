package simpledb;

import simpledb.Predicate.Op;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    public SegmentAdder Bucks;
    public int buckets,min,max;
    public double eps = 1e-5;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.Bucks = new SegmentAdder(buckets);
    }

    /**
     * Uses Long because the 2 search needs more range of <B>v</B>
     * @param v Given Value
     * @return The Index of the Given Value
     */
    public long getIndex(long v){
        // return buckets*(v-min)/(this.iPB)+1;
        if (v<min) return 0;
        if (v>max) return buckets+1;
        return (long)Math.floor((v-min+eps)/(max-min+eps*2)*buckets)+1;
    }
    public double getIndexDouble(long v){
        // return buckets*(v-min)/(this.iPB)+1;
        return Math.floor((v-min+eps)/(max-min+eps*2)*buckets)+1;
    }

    /**
     * To Cover the whole int range, make it bigger
     * @param i bucket number
     * @return min value
     */
    public long findMini(long i){
        long mv = ((long)1)<<40;
        long l = -mv;
        long r = mv;
        // System.out.println("l:"+l+" r:"+r);
        // index(l)<i<=index(r)
        while (l+1<r){
            long m = (l+r)/2;
            // System.out.println(l+","+m+","+r);
            if (i<=getIndex(m)){
                r = m;
            }else{
                l = m;
            }
        }
        // System.out.println("res:"+r);
        return r;
    }
    /**
     * Unused Debug Model
     * @param i
     * @return
     */
    public int findMiniPrint(int i){
        int mv = 1<<29;
        int l = -mv;
        int r = mv;
        // index(l)<i<=index(r)
        while (l+1<r){
            int m = (l+r)/2;
            System.out.println("find"+i+":"+l+","+m+","+r+","+getIndex(m)+","+getIndexDouble(m));
            if (i<=getIndex(m)){
                r = m;
            }else{
                l = m;
            }
        }
        return r;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        Bucks.addAt((int)getIndex(v), 1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double ans = 0;
        // some code goes here
        if (op==Op.EQUALS){
            long i = getIndex(v);
            long l = findMini(i);
            long r = findMini(i+1);
            double tmp = this.Bucks.get((int)i);
            ans = tmp / (r-l);
            return ans/Bucks.sum();
        } else if (op==Op.GREATER_THAN){
            long i = getIndex(v);
            long l = findMini(i);
            long r = findMini(i+1);
            double tmp = this.Bucks.get((int)i);
            ans += this.Bucks.getRange(i+1, buckets);
            // System.out.println(ans+","+l+","+r+","+i);
            ans += tmp * (double)(r-v-1)/(r-l);
            // System.out.println(tmp);
            return ans/Bucks.sum();
        } else if (op==Op.GREATER_THAN_OR_EQ){
            long i = getIndex(v);
            long l = findMini(i);
            long r = findMini(i+1);
            double tmp = this.Bucks.get((int)i);
            ans += this.Bucks.getRange(i+1, buckets);
            ans += tmp * (double)(r-v)/(r-l);
            return ans/Bucks.sum();
        } else if (op==Op.LESS_THAN){
            long i = getIndex(v);
            long l = findMini(i);
            long r = findMini(i+1);
            double tmp = this.Bucks.get((int)i);
            ans += this.Bucks.getRange(1, i-1);
            ans += tmp * (double)(v-l)/(r-l);
            return ans/Bucks.sum();
        } else if (op==Op.LESS_THAN_OR_EQ){
            long i = getIndex(v);
            long l = findMini(i);
            long r = findMini(i+1);
            double tmp = this.Bucks.get((int)i);
            ans += this.Bucks.getRange(1, i-1);
            ans += tmp * (double)(v-l+1)/(r-l);
            return ans/Bucks.sum();
        } else if (op==Op.NOT_EQUALS){
            long i = getIndex(v);
            long l = findMini(i);
            long r = findMini(i+1);
            double tmp = this.Bucks.get((int)i);
            ans += this.Bucks.sum();
            ans -= tmp * (double)(1)/(r-l);
            return ans/Bucks.sum();
        } else throw new NotImplementedException();
        //TODO
        // return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "(b:"+this.buckets+", min:"+min+", max:"+max+")";
        // return null;
    }

    public class SegmentAdder{
        private int [] value;
        int sz;
        SegmentAdder(int n){
            sz = 1;
            while (sz<n+2){
                sz <<=1;
            }
            value = new int[sz*2+1];
            for (int i=0;i<=sz*2;++i){
                value[i] = 0;
            }
        }
        public int get(int i){
            return value[i+sz];
        }
        public void addAt(int i,int v){
            int pos = i + sz;
            while (pos>=1){
                value[pos]+=v;
                pos>>=1;
            }
        }
        public int getRange(long i, long j){
            if (i>j) return 0;
            if (i<1) i = 1;
            if (j>sz-1) j = sz-1;
            int l = (int)i+sz-1;
            int r = (int)j+sz+1;
            int lval = 0;
            int rval = 0;
            while ((l>>1)!=(r>>1)){
                if ((l&1)==0){
                    lval+=value[l+1];
                }
                if ((r&1)==1){
                    rval+=value[r-1];
                }
                r>>=1;
                l>>=1;
            }
            return lval+rval;
        }
        public int simpleRange(int i,int j){
            int sum = 0;
            for (int k=i;k<=j;++k){
                sum+=get(k);
            }
            return sum;
        }
        public String View(){
            String ans = "";
            for (int i=1;i<=sz*2-1;++i){
                if (Integer.bitCount(i)==1) ans += "\n";
                ans += value[i];
            }
            return ans;
        }
        public int sum(){
            return value[1];
        }
    }
}
