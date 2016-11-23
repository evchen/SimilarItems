import java.io.Serializable;

/**
 * Created by root on 11/12/16.
 */
public class Pair extends Object implements Serializable, Comparable<Pair>  {

    Long COL1;
    Long COL2;

    public Pair(Long col1, Long col2){
        this.COL1=col1;
        this.COL2=col2;
    }


    public int compareTo(Pair p){
        if(COL1==p.COL1 && p.COL2==COL2){
            return 0;
        }
        else if(p.COL1.equals(COL2) && p.COL2.equals(COL1))
        {
            return 0;
        }
        else return 1;
    }


    public boolean equals(Pair p){
        if(COL1==p.COL1 && p.COL2==COL2){
            return true;
        }
        else if(p.COL1.equals(COL2) && p.COL2.equals(COL1))
        {
            return true;
        }
        return false;
    }
    public String toString(){
        return COL1 + " + " +COL2;
    }
}
