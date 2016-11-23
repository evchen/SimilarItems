import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

/**
 * Created by root on 11/9/16.
 */
public class MinHashing implements Serializable {

    static String[] name;
    int K;
    JavaSparkContext sc;
    static int PERMUT;


    static JavaRDD<Integer>[] shingle_array;
    static JavaRDD<Integer> u_shingle;
    static JavaPairRDD<Integer,Long> union_index;


    public MinHashing(String name[], JavaSparkContext sc, int K, int PERMUT){
        this.name = name;
        this.K = K;
        this.sc = sc;
        this.PERMUT = PERMUT;

        construct_sh();
        union();

    }

    private void construct_sh(){
        Shingling shg = new Shingling(K);
        shingle_array = new JavaRDD[name.length];
        for(int i = 0; i<name.length; i++)
        {
            shingle_array[i]=shg.makeHashedShingles(name[i],sc);
        }
    }

    private void union(){
        u_shingle = shingle_array[0];
        for (JavaRDD<Integer> sh: shingle_array
             ) {
            u_shingle = u_shingle.union(sh).distinct();
        }
    }



    public static  JavaRDD<Integer>[] getArray(){
        return shingle_array;
    }

    private static JavaRDD<Integer> sort_shingle(JavaRDD<Integer> sh){
        JavaRDD<Integer> sort_sh= sh.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer v){
                return v;
            }
        },true, 1);
        return sort_sh;
    }


    private static Long[][] sig;


    public static Long[][] getSig(){
        sig = new Long[PERMUT][name.length];
        createSig();
        return sig;
    }


    private static void createSig(){
        for(int per= 0; per<PERMUT; per++ )
            create1dArray(per);

    }

    private static void create1dArray(int per){
        // hashing

        RandomHash rh = new RandomHash();

        //gen_ab();
        for(int i= 0; i<name.length;i++){
            shingle_array[i]=sort_shingle(rh.hash_integer(shingle_array[i]));

        }

        u_shingle = rh.hash_integer(u_shingle);
        u_shingle = sort_shingle(u_shingle);

        // index
        union_index = u_shingle.zipWithIndex();

        // get first
        Integer[] array = new Integer[name.length];
        for(int i = 0; i<name.length; i++)
        {
            array[i]=shingle_array[i].first();
        }

        // get index
        for(int i = 0; i<name.length; i++){
            List<Long> l = union_index.lookup(array[i]);
            sig[per][i] = l.get(0)+1;
        }


    }

    public static void print_shingles(JavaRDD<Integer> sh){
        List<Integer> hash_list1 = sh.collect();

        ListIterator<Integer> li = hash_list1.listIterator();

        while(li.hasNext()){
            System.out.println(li.next());
        }
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        createSig();
        for(int i = 0; i<sig.length ; i++){
            for (int j = 0; j<sig[0].length; j++){
                sb.append(sig[i][j]+" ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }


}
