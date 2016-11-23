import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by root on 11/8/16.
 */
public class Shingling implements Serializable {

    String name;
    static int S_LENGTH;


    public Shingling(int S_LENGTH){

        this.S_LENGTH = S_LENGTH;
    }

    private static Iterator<String> makeList(String s){
        LinkedList<String> l = new LinkedList();
        ListIterator<String> li = l.listIterator();
        int i = 0;
        while(s.length()-i>=S_LENGTH){
            li.add(s.substring(i,i+S_LENGTH));
            i++;
        }

        return l.iterator();
    }


    public JavaRDD<String> makeShingles(String name,JavaSparkContext sc){


        JavaRDD<String> textFile = sc.textFile(name);
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) { return makeList(s); }
        });
        JavaRDD<String> shingles = words.distinct();

        return shingles;
    }

    public JavaRDD<Integer> makeHashedShingles(String name, JavaSparkContext sc){
        JavaRDD<String> s = makeShingles(name, sc);
        JavaRDD<Integer> hash_shingles = s.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.hashCode(); }
        });
        JavaRDD<Integer> sort_shingles = hash_shingles.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer v){
                return v;
            }
        },true, 1);
        return sort_shingles;
    }



}
