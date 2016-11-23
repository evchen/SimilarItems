import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;


import java.util.*;


/**
 * Created by root on 11/12/16.
 */
public class LSH {

    static Long[][] signature_matrix;

    static int NOF_DOC;

    static double t;

    static int ROW = 1;

    static int N;
    static JavaSparkContext sc;

    static int BAND;

    public LSH(Long[][] signature_matrix, double t, int N,JavaSparkContext sc){
        this.signature_matrix = signature_matrix;
        this.t = t;
        NOF_DOC = signature_matrix[0].length;
        this.N = N;
        this.sc = sc;
        BAND = N/ROW;
    }

    public static List<String> get_candidate_pairs(){
        Map<String,Long> map= map_pairs();
        List<String> list= new LinkedList<String>();

        for(Map.Entry<String, Long> entry : map.entrySet()) {
            String pair = entry.getKey();
            Long count = entry.getValue();
            double fraction = (double)count/(double)BAND;

            System.out.println("fraction: "+fraction+" count: "+count + " pair: "+pair + " band: " +BAND);
            if(fraction>=t){
                list.add(pair);
            }
        }
        return list;

    }

    public static Map<String,Long> map_pairs(){
        JavaRDD<Pair> pairs = sc.parallelize(new LinkedList<Pair>());
        // get all bands
        int start = 0;
        while (start<N){
            List<List> vectors = createVectors(start);
            JavaRDD<List> rddvectors = createRDDvectors(vectors);
            JavaRDD<Integer> hashed = hashVector(rddvectors);
            JavaPairRDD<Integer, Iterable<Long>> bucket = make_bucket(hashed);
            JavaRDD<Pair> temp = getPairs(bucket);
            pairs = pairs.union(temp);
            start+=ROW;
        }
        JavaRDD<String> pair_name = pairs.map(new Function<Pair, String>() {
            public String call(Pair p) throws Exception {
                return p.toString();
        }});
        return pair_name.countByValue();
    }

    public static List<List> createVectors(int start){
        LinkedList<List> outerList = new LinkedList();

        for (int i =0; i<NOF_DOC; i++) {

            LinkedList<Long> innerList = new LinkedList();

            int j = 0;

            while(j<ROW)
            {
                innerList.add(signature_matrix[j][i]);
                j++;
            }
            outerList.add(innerList);
        }


        return outerList;

    }

    public static JavaRDD<List> createRDDvectors(List<List> list){

        JavaRDD<List> rdd_list =sc.parallelize(list);
        return rdd_list;
    }

    public static JavaRDD<Integer> hashVector(JavaRDD<List> list){
        RandomHash rh = new RandomHash();
        JavaRDD<Integer> hash_vector_rdd = list.map(new Function<List, Integer>() {
            public Integer call(List list) throws Exception {
                
                return list.hashCode();
            }
        });

        JavaRDD<Integer> random_vector = rh.hash_integer(hash_vector_rdd);

        return random_vector;

    }

    public static JavaPairRDD<Integer,Iterable<Long>> make_bucket(JavaRDD<Integer> hashed_list){
        JavaPairRDD<Integer, Long> indexed_list = hashed_list.zipWithIndex();
        JavaPairRDD<Integer, Iterable<Long>> reduced_list = indexed_list.groupByKey();
        return reduced_list;
    }

    public static JavaRDD<Pair> getPairs(JavaPairRDD<Integer,Iterable<Long>> bucket){
        JavaRDD<Iterable<Long>> similars = bucket.values().filter(new Function<Iterable<Long>, Boolean>() {
            public Boolean call(Iterable<Long> longs) throws Exception {
                Iterator<Long> it = longs.iterator();
                Long item = it.next();
                return it.hasNext();
            }
        });

        JavaRDD<Pair> pairs = similars.flatMap(
                new FlatMapFunction<Iterable<Long>, Pair>() {

                    public Iterator<Pair> call(Iterable<Long> s) { return convert(s); }
                });



        return pairs;
    }

    private static Iterator<Pair> convert(Iterable<Long> longs ){
        Iterator<Long> it = longs.iterator();
        List<Long> list = new ArrayList<Long>();
        while (it.hasNext()){
            Long el =it.next();
            list.add(el);
        }


        int i = 0;
        List<Pair> pairs = new LinkedList<Pair>();
        while(i<list.size()-1){
            int j = i+1;
            while(j<list.size()){

                pairs.add(new Pair(list.get(i),
                        list.get(j)));

                j++;
            }
            i++;
        }


        return pairs.listIterator(0);
    }


}
