import java.util.Vector;

public class nulltest {
    public static void main(String args[]){
        Vector<Integer> vec1 = new Vector<>();
        Vector<Integer> vec2 = new Vector<>();
        System.out.println(vec1.getClass()==vec2.getClass());
    }
}