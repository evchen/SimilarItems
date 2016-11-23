import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by root on 11/8/16.
 */
public class CompareSets {

    String name1;
    String name2;
    JavaSparkContext sc;
    int k;
    JavaRDD<String> shingle1;
    JavaRDD<String> shingle2;

    public CompareSets(String name1, String name2, JavaSparkContext sc, int k){
        this.name1 = name1;
        this.name2 = name2;
        this.sc = sc;
        this.k = k;
        Shingling shg = new Shingling(k);
        this.shingle1 = shg.makeShingles(name1,sc);
        this.shingle2 = shg.makeShingles(name2,sc);
    }

    public long countUnion(){


        JavaRDD<String> union_init = shingle1.union(shingle2);
        JavaRDD<String> union = union_init.distinct();

        return union.count();
    }

    public long countIntersect(){
        JavaRDD<String> intersect = shingle1.intersection(shingle2);
        return intersect.count();
    }

    public double calc_Jac_sim(){
        System.out.println(this.countIntersect()+ "+" + this.countUnion());
        return (double)this.countIntersect()/(double)this.countUnion();
    }

}
