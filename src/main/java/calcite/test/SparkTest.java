package calcite.test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

public class SparkTest {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        //JavaRDD<Integer> distData = sc.parallelize(data);
        JavaRDD<String> distFile = sc.textFile("src/main/resources/data.txt");
        System.out.println(distFile.collect());
    }

}
