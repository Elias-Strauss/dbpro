package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.apache.spark.api.java.function.PairFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

public class JSONtoSpark {

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    Map<Integer,JavaRDD<String>> rddplusID = new HashMap<>();

    PairFunction<String, String, String> keyDataLeft = new PairFunction<String, String, String>() {
        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            //String[] cols = s.split(",");
            //return new Tuple2(cols[colIDLeft],s);
            return null;
        }
    };

    public JavaRDD<String> translate(){

        try {
            Path filePath = Path.of("src/main/resources/output.json");
            String jsonContent = Files.readString(filePath);

            JSONArray jsonArray = new JSONArray(jsonContent);
            System.out.println(jsonArray);
            JSONObject fst = (JSONObject) jsonArray.get(8);
            System.out.println(fst);
            for (Object o : jsonArray){
                JSONObject current = (JSONObject) o;
                String type = (String) current.get("type");

                if (type.equals("TableScan")){
                    String name = (String) current.get("TableName");
                    JavaRDD<String> distFile = sc.textFile("src/main/resources/"+name+" - Copy.csv");
                    int id = Integer.parseInt((String) current.get("id"));
                    rddplusID.put(id,distFile);
                    //System.out.println(distFile.collect());
                }

                if (type.equals("TableModify")){
                    System.out.println(type);
                }

                //for now no commas allowed in the fields
                if (type.equals("Project")){
                    int id = Integer.parseInt((String) current.get("id"));
                    int idInput = Integer.parseInt((String) current.get("id_a"));
                    JavaRDD<String> input = rddplusID.get(idInput);
                    if (input != null){
                        JSONArray colIDs = (JSONArray) current.get("colIDs");
                        //System.out.println(colIDs);
                        //System.out.println(input.collect());
                        JavaRDD<String> output = input.map(x-> {
                            String output2 = "";
                            String[] cols = x.split(",");
                            for(Object o2 : colIDs){
                                String colID = (String) o2;
                                if (output2.equals(""))output2=cols[Integer.parseInt(colID)];
                                else output2=output2+","+cols[Integer.parseInt(colID)];
                            }
                            return output2;
                        });
                        //System.out.println(output.collect());
                        rddplusID.put(id,output);
                        //System.out.println(rddplusID);
                    }
                }

                if (type.equals("Filter")){
                    int id = Integer.parseInt((String) current.get("id"));
                    int idInput = Integer.parseInt((String) current.get("id_a"));
                    int colID = Integer.parseInt((String) current.get("colID"));
                    int conditionVal = Integer.parseInt((String) current.get("conditionVal"));
                    String condition = (String)current.get("condition");


                    JavaRDD<String> input = rddplusID.get(idInput);
                    if (input != null){
                        //System.out.println(input.collect());
                        JavaRDD<String> output = input.filter(x-> {
                            String[] cols = x.split(",");
                            if (condition.equals(">")){
                                return Float.parseFloat(cols[colID]) > conditionVal;
                            }
                            if (condition.equals("<")){
                                return Float.parseFloat(cols[colID]) < conditionVal;
                            }
                            if (condition.equals("=")){
                                return Float.parseFloat(cols[colID]) == conditionVal;
                            }
                            return false;
                        });
                        //System.out.println(output.collect());
                        rddplusID.put(id,output);
                        //System.out.println(rddplusID);

                    }


                }

                if (type.equals("Join")){
                    String joinType = (String) current.get("joinType");
                    if (joinType.equals("INNER")){
                        int id = Integer.parseInt((String) current.get("id"));
                        int idLeft = Integer.parseInt((String) current.get("id_a"));
                        int idRight = Integer.parseInt((String) current.get("id_b"));
                        int colIDLeft = Integer.parseInt((String) current.get("colIDLeft"));
                        int colIDRight = Integer.parseInt((String) current.get("colIDRight"));
                        JavaRDD<String> inputLeft = rddplusID.get(idLeft);
                        System.out.println(inputLeft.collect());
                        JavaRDD<String> inputRight = rddplusID.get(idRight);
                        System.out.println(inputRight.collect());
                        JavaPairRDD<String,String> pairsLeft = TupleSerializable.tupleHelper(inputLeft,colIDLeft);
                        JavaPairRDD<String,String> pairsRight = TupleSerializable.tupleHelper(inputRight,colIDRight);

                        /*System.out.println(colIDLeft);
                        System.out.println(colIDRight);
                        System.out.println(pairsLeft.collect());
                        System.out.println(pairsRight.collect());*/
                        //System.out.println(inputLeft.collect());
                        JavaRDD<String> output = pairsLeft.join(pairsRight).values().map(x->x._1+","+x._2);
                        System.out.println(output.collect());
                        rddplusID.put(id,output);

                    }else{
                        System.out.println("panikjoinnotinner");
                    }

                }

                if (type.equals("Sort")){
                    int id = Integer.parseInt((String) current.get("id"));
                    int idInput = Integer.parseInt((String) current.get("id_a"));
                    int colID = Integer.parseInt((String) current.get("sortKey"));
                    String order = (String) current.get("order");
                    boolean ordBool = false;
                    if (order.equals("ASC")) ordBool=true;
                    JavaRDD<String> input = rddplusID.get(idInput);
                    if (input!=null) {
                        JavaPairRDD<String, String> pair = TupleSerializable.tupleHelper(input, colID);
                        //System.out.println(pair.sortByKey(ordBool).values());
                        JavaRDD<String> output = pair.sortByKey(ordBool).values();
                        rddplusID.put(id,output);
                    }
                }

                if (type.equals("Aggregate")){
                    System.out.println(type);
                }
            }

            /*Iterator iterator = subjects.iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }*/
        } catch(Exception e) {
            e.printStackTrace();
        }



        //JavaRDD<String> distFile = sc.textFile("src/main/resources/"+"output"+".json");

        return null;
    }

    /*public JavaPairRDD<String,String> tupleHelper (JavaRDD<String> rdd, int colID){
        return rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] cols = s.split(",");
                return new Tuple2(cols[colID],s);
            }
        });

    }*/

}
