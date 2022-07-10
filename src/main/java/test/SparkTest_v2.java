package test;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;

public class SparkTest_v2 {

    public static void main (String[] args) {
        /*SparkConf conf = new SparkConf().setAppName("TestApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Reste\\OneDrive\\Desktop\\dvdrental_public_actor.csv");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(lines.collect());*/
        JSONtoSpark_v2 temp = new JSONtoSpark_v2();
        JavaRDD<ArrayList<Object>> result = temp.translate();

        System.out.println("------------------------------------------------\n");
        result.take(10).forEach(System.out::println);
        System.out.println("\n------------------------------------------------");
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> rdd = sc.textFile("src/main/resources/TPC-HTestDaten/part.tbl");
//        JavaRDD<String[]> rddArray = rdd.map(row -> row.split("\\|"));
//        rdd = rddArray.map(array -> array[1]);
//        System.out.println(rdd.collect());
    }


}
