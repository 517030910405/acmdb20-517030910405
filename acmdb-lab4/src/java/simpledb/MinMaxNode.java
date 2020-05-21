package simpledb;

import simpledb.Aggregator.Op;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MinMaxNode {
    
    public int max,min,sum,cnt;
    public MinMaxNode(){
        max = Integer.MIN_VALUE;
        min = Integer.MAX_VALUE;
        sum = 0;
        cnt = 0;
    }
    public void add(int value){
        max = Integer.max(max, value);
        min = Integer.min(min, value);
        sum += value;
        cnt += 1;
    }
    public boolean isAvailable(){
        return cnt>0;
    }
    public Field getValue(Op what){
        if (what.equals(Op.MAX)){
            return new IntField(max);
        } else if (what.equals(Op.MIN)){
            return new IntField(min);
        } else if (what.equals(Op.COUNT)){
            return new IntField(cnt);
        } else if (what.equals(Op.SUM)){
            return new IntField(sum);
        } else if (what.equals(Op.AVG)){
            return new IntField(sum/cnt);
        }
        throw new NotImplementedException();
        // return null;
    }

}