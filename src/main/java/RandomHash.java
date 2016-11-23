import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Random;

/**
 * Created by root on 11/12/16.
 */
public class RandomHash {

    static int A = 0;
    static int B = 0;
    final static Integer C= 2147483647;

    public RandomHash(){
        gen_ab();

    }

    private static void gen_ab(){

        Random ran = new Random();
        A = ran.nextInt(5000);
        B = ran.nextInt(5000);
        //System.out.println("A " + A + " B " + B );
    }


    public static JavaRDD<Integer> hash_integer(JavaRDD<Integer> sh){

        JavaRDD<Integer> hash_sh = sh.map(new Function<Integer, Integer>() {
            public Integer call(Integer v){
                Integer result = (v*A+B)%C;
                return  result;
            }
        });
        return hash_sh;
    }






}
