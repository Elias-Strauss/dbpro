package test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class TupleSerializable implements Serializable {

    public static JavaPairRDD<String,String> tupleHelper (JavaRDD<String> rdd, int colID){
        return rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                //System.out.println(s);
                //System.out.println(colID);
                String[] cols = s.split(",");
                return new Tuple2(cols[colID],s);
            }
        });

    }

}
