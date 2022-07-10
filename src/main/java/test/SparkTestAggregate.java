package test;

import java.awt.desktop.SystemSleepEvent;
import java.util.*;
import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.util.StatCounter;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SparkTestAggregate {

    public static void main(String[] args) {
        SparkConf conf2 = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf2);


        List myList = Arrays.asList(
                new Tuple2<>("A", new int[] {2, 7}),
                new Tuple2<>("B", new int[] {5, 7}),
                new Tuple2<>("C", new int[] {10, 3}),
                new Tuple2<>("A", new int[] {8, 4}),
                new Tuple2<>("B", new int[] {2, 5}));
        JavaPairRDD<String, int[]>  pairs = sc.parallelizePairs(myList);
                pairs
                        .aggregateByKey(new ArrayList(), (acc, x) ->  {
                            ArrayList list = new ArrayList<>();
                            list.add(x[0]);
                            list.add(x[1]);
                            return list;
                            }, (acc1, acc2) -> {
                            return acc2;
                        }).collect().forEach(t -> {
                            System.out.println(t._1 + t._2.toString());
                        });
                        //.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()))
                        //.collect()

    }
}