package test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

public class SparkTest {

    public static void main (String[] args) {
        SparkConf conf = new SparkConf().setAppName("TestApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Reste\\OneDrive\\Desktop\\actor.csv");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        lines.collect().forEach(line -> System.out.println(line));
    }


}
