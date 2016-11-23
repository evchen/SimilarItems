import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by root on 11/8/16.
 */
public class HashTest {
    static String[] st1= {"this is a string","this is a string too","this is not the same string"};
    static String[] st2= {"this is a string","this is a string in 2", "this is the 3rd string"};


    public static void main(String[] arg){


        long[][] array = {{2,3,4},{1,2,5},{2,3,5},{4,7,9}};

        System.out.println(array.length);
        System.out.println(array[0].length);

        LinkedList<List> outerList = new LinkedList();

        for (int i =0; i<3; i++) {

            LinkedList<Long> innerList = new LinkedList();

            int j = 0;

            while(j<4)
            {
                innerList.add(array[j][i]);
                j++;
            }
            outerList.add(innerList);
        }
        ListIterator<List> li_o = outerList.listIterator();
        while(li_o.hasNext()){
            ListIterator<Long> li_i =li_o.next().listIterator();
            while(li_i.hasNext())
            {
                System.out.print(li_i.next()+"\t");
            }
            System.out.println();
        }


        //int a = 304856787;
        //int b = 27354678;

        //System.out.println(a^b);
        /*
        System.out.println(st1[0].hashCode());
        System.out.println(st1[1].hashCode());
        System.out.println(st1[2].hashCode());
        System.out.println(st1[0].hashCode());
        System.out.println(st2[0].hashCode());
        System.out.println(st2[1].hashCode());
        System.out.println(st2[2].hashCode());
        */
    }

}
