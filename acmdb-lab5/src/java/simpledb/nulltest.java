import java.util.Vector;
import java.lang.*;
public class nulltest {
    public static void main(String args[]){
        Vector<Integer> vec1 = new Vector<>();
        Vector<Integer> vec2 = new Vector<>();
        System.out.println(vec1.getClass()==vec2.getClass());
        // System.out.println((int)(1.9));
        int a = 1;
        if (a==1) a=2;
        
        System.out.println();
    }
}