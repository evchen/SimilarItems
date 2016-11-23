/**
 * Created by root on 11/7/16.
 */

import com.google.common.hash.Hashing;
import org.apache.avro.io.parsing.Symbol;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.util.SystemClock;

import java.util.*;


public class Test {
    static int S_LENGTH = 5;
    static int PERMUTATION = 10;


    public static void main(String[] arg){

        // initialization
        SparkConf conf = new SparkConf().setAppName("local").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");
        //Shingling shg = new Shingling(S_LENGTH);

        // create string shingles
        //JavaRDD<String> shingle1 = shg.makeShingles("Testing.txt",sc);
        //JavaRDD<String> shingle2 = shg.makeShingles("Testing2.txt",sc);

        // create hashed shingles
        //JavaRDD<Integer> shingle1 = shg.makeHashedShingles("Testing.txt",sc);
        //JavaRDD<Integer> shingle2 = shg.makeHashedShingles("Testing2.txt",sc);

        String[] array = {"Testing.txt","Testing2.txt","Testing3.txt","Testing4.txt"};
        MinHashing min = new MinHashing(array, sc,S_LENGTH,PERMUTATION);
        Long[][] sig = min.getSig();
        System.out.println(min.toString());

        LSH lsh = new LSH(sig,0.1,PERMUTATION,sc);
        System.out.println(lsh.get_candidate_pairs().size());


/*
        Pair p1 = new Pair(new Long(0), new Long(1));
        Pair p2 = new Pair(new Long(0), new Long(1));

        Long l1 = new Long(0);
        Long l2 = new Long(0);
        System.out.println(l1.equals(l2));
        System.out.println(p1.equals(p2));

        List<Pair> list = new LinkedList<Pair>();
        list.add(p1);
        list.add(p2);
        JavaRDD<Pair> pair = sc.parallelize(list);
        System.out.println(pair.countByValue().size());

        List<String> list1 = new LinkedList<String>();
        list1.add("colknfsdf");
        list1.add("colknfsdf");
        JavaRDD<String> pair1 = sc.parallelize(list1);
        System.out.println(pair1.countByValue().size());




        /*
        for(int i=0; i<PERMUTATION; i++) {
            for (int j = 0; j < array.length; j++)
                System.out.print(sig[i][j] + "\t");
            System.out.println();
        }
        /*




        CompareSignatures cs = new CompareSignatures(sig);
        ListIterator<CompareSignatures.Pair> li= cs.getSimilarPairs().listIterator();
        while(li.hasNext()){
            System.out.println(li.next());
        }




        /*
        JavaRDD<String> union_init = shingle1.union(shingle2);
        JavaRDD<String> union = union_init.distinct();

        JavaRDD<String> intersect = shingle1.intersection(shingle2);

        CompareSets cs = new CompareSets("Testing.txt","Testing2.txt",sc,S_LENGTH);
        */
        /*
        List<Integer> hash_list1 = shingle1.collect();

        ListIterator<Integer> li = hash_list1.listIterator();

        while(li.hasNext()){
            System.out.println(li.next());
        }


        JavaPairRDD<Integer, Long> s_index = shingle1.zipWithIndex();
        List<Long> list = s_index.lookup(110469124);
        System.out.println(list.get(0));
        */

    }


    public static void print_shingles(JavaRDD<Integer> sh){
        List<Integer> hash_list1 = sh.collect();

        ListIterator<Integer> li = hash_list1.listIterator();

        while(li.hasNext()){
            System.out.println(li.next());
        }
    }


//        System.out.println(new CompareSets().countUnion("Testing.txt","Testing2.txt",sc,S_LENGTH) + " + " + intersect.count());

/*
        List<String> ls = shingles.collect();
        ListIterator<String> li = ls.listIterator();
        while(li.hasNext()){
            System.out.println(li.next());
        }
        //System.out.println(shingles.first());
        */



/*
    public static void main(String[] arg){

        // Initializing
        SparkConf conf = new SparkConf().setAppName("local").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Convert to JavaRDD file
        JavaRDD<String> textFile = sc.textFile("Testing.txt");
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return makeList(s); }
        });
        JavaRDD<String> shingles = words.distinct();
        JavaRDD<Integer> hash_shingles = shingles.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.hashCode(); }
        });

        System.out.println(shingles.first().hashCode());
        System.out.println(hash_shingles.first());
        System.out.println(words.count());
        System.out.println(shingles.count());
    }

    public static List<String> makeList(String s){
        LinkedList<String> l = new LinkedList();
        ListIterator<String> li = l.listIterator();
        int i = 0;
        while(s.length()-i>=S_LENGTH){
            li.add(s.substring(i,i+S_LENGTH));
            i++;
        }

        return l;
    }
    /*
    public static void main(String[] args) {
        int NUM_SAMPLES = 10000;

        SparkConf conf = new SparkConf().setAppName("local").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }



        long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer i) {
                double x = Math.random();
                double y = Math.random();
                return x*x + y*y < 1;
            }
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
*/
}