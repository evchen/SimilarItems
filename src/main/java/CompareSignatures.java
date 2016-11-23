import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 11/11/16.
 */
public class CompareSignatures {

    Long[][] sig_matrix;

    public CompareSignatures(Long[][] sig){
        this.sig_matrix = sig;
    }

    public List<Pair> getSimilarPairs(){
        List<Pair> sim = new ArrayList<Pair>();
        for(int i=0; i<sig_matrix[0].length-1; i++){
            for(int j=i+1; j<sig_matrix[0].length; j++){
                int count = 0;
                int k=0;
                while(k<=sig_matrix.length/sig_matrix[0].length)
                {
                    if (sig_matrix[k][i].equals(sig_matrix[k][j]))
                    {
                        count++;
                    }
                    k++;
                }

                double similarity = count/(sig_matrix.length/sig_matrix[0].length);
                if (similarity>=0.8){
                    Pair p = new Pair(new Long(i),new Long(j));
                    sim.add(p);
                }

            }
        }

        return sim;
    }




}
