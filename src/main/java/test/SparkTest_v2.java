package test;

import execution_engines.JSONtoSpark_v2;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkTest_v2 {

    public static void main (String[] args) throws IOException {
        /*SparkConf conf = new SparkConf().setAppName("TestApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Reste\\OneDrive\\Desktop\\dvdrental_public_actor.csv");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(lines.collect());*/
        Path filePath = Path.of("src/main/resources/q1-orca2.json");
        String jsonContent = Files.readString(filePath);
        JSONtoSpark_v2 temp = new JSONtoSpark_v2();
        ArrayList<Integer> mili = new ArrayList<>();
        for (int i = 0; i < 1; i++){
            JavaRDD<ArrayList<Object>> result = temp.translate(jsonContent);

            System.out.println("------------------------------------------------\n");
            result.take(10).forEach(System.out::println);
            System.out.println("\n------------------------------------------------");

            System.out.println(temp.stopwatch.elapsed(TimeUnit.MILLISECONDS));
            mili.add((int) temp.stopwatch.elapsed(TimeUnit.MILLISECONDS));
            temp.stopwatch.reset();
        }

        AtomicInteger all = new AtomicInteger();
        mili.forEach(m -> {
            all.set(m + all.get());
        });
        System.out.println(all.get() / 1);
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> rdd = sc.textFile("src/main/resources/TPC-HTestDaten/part.tbl");
//        JavaRDD<String[]> rddArray = rdd.map(row -> row.split("\\|"));
//        rdd = rddArray.map(array -> array[1]);
//        System.out.println(rdd.collect());
    }


}
