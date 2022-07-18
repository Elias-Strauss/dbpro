package execution_engines;

import com.google.common.base.Stopwatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import test.CQuery;

public class JSONtoSpark_v2 implements Serializable {

    SparkConf conf;
    JavaSparkContext sc;
    public Stopwatch stopwatch = Stopwatch.createUnstarted();
    public JSONtoSpark_v2() {
        this.conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        this.sc = new JavaSparkContext(this.conf);
        sc.setLogLevel("ERROR");
    }


    public JavaRDD<ArrayList<Object>> translate(String jsonString){

        try {
            JSONArray jsonArray = JSONArray.parseArray(jsonString);

            //this.stopwatch.start();
            JavaRDD<ArrayList<Object>> returnRdd = null;

            CQuery cQuery = new CQuery(jsonArray);

            while (cQuery.isNotDone()) {

                JSONObject current = cQuery.getNext();
                String type = current.getString("type");
                String name = current.getString("TableName");
                int id = current.getIntValue("id", -1);
                int id_a = current.getIntValue("id_a", -1);

                //TODO: Join Logic

                switch (type) {
                    case "TableScan":
                        //TODO: get proper Table
                        returnRdd = sc.textFile("src/main/resources/TPC-HTestDaten/" + current.getString("name") + ".tbl")
                                .map(row -> new ArrayList<>(Arrays.asList(row.split("\\|"))));

                    break;

                    case "TableModify":
                        System.out.println(type);
                        break;

                    case "Projection":
                        JSONArray colIDs = (JSONArray) current.get("colIDs");

                        assert returnRdd != null;
                        returnRdd = returnRdd.map(row -> {
                            ArrayList<Object> newRow = new ArrayList<Object>();

                            for (int i = 0; i < colIDs.size(); i++) {
                                //todo: type for calculation in projection
                                if (colIDs.get(i).getClass().equals(JSONObject.class)) {
                                    newRow.add(i, calcOperators((JSONObject) colIDs.get(i), row));
                                } else {
                                    newRow.add(i, row.get(colIDs.getIntValue(i)));
                                }

                            }
                            return newRow;
                        });

                        break;

                    case "Filter":
                        JSONArray conditions = current.getJSONArray("cond");

                        assert returnRdd != null;
                        returnRdd = returnRdd.filter(row -> {
                            final boolean[] result = {true};
                            conditions.forEach(conditionObj -> {
                                JSONObject condition = (JSONObject) conditionObj;

                                Object left;
                                Object right;

                                if (condition.getJSONObject("left").containsKey("colID")) {
                                    left = row.get(condition.getJSONObject("left").getIntValue("colID"));
                                } else {
                                    left = condition.getJSONObject("left").get("value");
                                }

                                if (condition.getJSONObject("right").containsKey("colID")) {
                                    right = row.get(condition.getJSONObject("right").getIntValue("colID"));
                                } else {
                                    right = condition.getJSONObject("right").get("value");
                                }
                                //TODO: make more cases, pls in a class
                                switch (condition.getString("valueType")) {
                                    case "Date":
                                        switch (condition.getString("comp_op")) {
                                            case "<=":
                                                LocalDate leftDate = LocalDate.parse(left.toString());
                                                LocalDate rightDate = LocalDate.parse(right.toString());
                                                result[0] = result[0] && (leftDate.compareTo(rightDate) <= 0);
                                            default:
                                                break;
                                        }
                                    default:
                                        break;
                                }

                            });
                            return result[0];
                        });

                        break;

                    case "Join":

                        break;

                    case "Sort":

                        assert returnRdd != null;
                        JavaPairRDD<String, ArrayList<Object>> pairRDDS = JavaPairRDD.fromJavaRDD(
                                returnRdd.map(row -> {
                                    AtomicReference<String> group = new AtomicReference<>("");//new LinkedList<>();
                                    ArrayList<Object> sort = new ArrayList<>(row);

                                    current.getJSONArray("sorting_colIDs").forEach(groupColObj -> {
                                        JSONObject groupCol = (JSONObject) groupColObj;
                                        group.set(group.get().concat((String) row.get(groupCol.getIntValue("colID"))));
                                    });

                                    return new Tuple2<String, ArrayList<Object>>(group.get(), sort);
                                })
                        );

                        pairRDDS = pairRDDS.sortByKey(true);

                        returnRdd = pairRDDS.map(row -> {
                            ArrayList<Object> newRow = new ArrayList<Object>();
                            row._2.forEach(rObj -> {
                                if (rObj.getClass().equals(Object[].class)) {
                                    //is average
                                    Object[] r = (Object[]) rObj;
                                    newRow.add((Double) r[0] / (Double) r[1]);
                                } else {
                                    newRow.add(rObj);
                                }
                            });
                            return newRow;
                        });

                        break;
                    case "Aggregate":
                        //C0 -> c5
                        //(c4,c5),(C0 -> c5)
                        //-> (c4, c5),(aggre_cols)
                        //c4, c5, aggre_cols
                        assert returnRdd != null;
                        JavaPairRDD<ArrayList<Object>, ArrayList<Object>> pairRDD = JavaPairRDD.fromJavaRDD(
                                returnRdd.map(row -> {
                                    ArrayList<Object> group = new ArrayList<>();
                                    ArrayList<Object> aggregate = new ArrayList<>(row);

                                    current.getJSONArray("grouping_colIDs").forEach(groupCol -> {
                                        group.add(row.get(Integer.parseInt((String) groupCol)));
                                    });

                                    return new Tuple2<ArrayList<Object>, ArrayList<Object>>(group, aggregate);
                                })

                        );
                        ArrayList<Object> arrayList = new ArrayList<Object>();
                        current.getJSONArray("agg_colIDs").forEach(aggColObj -> {
                            //TODO other startValues
                            JSONObject aggCol = (JSONObject) aggColObj;
                            if(Objects.equals(aggCol.getString("type"), "average")){
                                arrayList.add(new Object[] {0.0, 0.0});
                            }else{
                                arrayList.add(0.0);
                            }
                        });
                        pairRDD = pairRDD.aggregateByKey(arrayList, (acc, x) ->  {
                            AtomicInteger counter = new AtomicInteger(0);
                            current.getJSONArray("agg_colIDs").forEach(aggColObj -> {
                                JSONObject aggCol = (JSONObject) aggColObj;
                                //TODO other startValues
                                switch(aggCol.getString("type")) {
                                    case "sum":
                                        //acc.set(counter.get(), Double.parseDouble((String) acc.get(counter.get())) + Double.parseDouble((String) x.get(aggCol.getIntValue("colID"))));
                                        String tmp = x.get(aggCol.getIntValue("colID")).toString();
                                        if (!tmp.contains(".")) {
                                            tmp = tmp.concat(".0");
                                        }
                                        acc.set(counter.get(), (Double) acc.get(counter.get()) + Double.parseDouble(tmp));
                                        break;
                                    case "average":
                                        String tmpA = x.get(aggCol.getIntValue("colID")).toString();
                                        if (!tmpA.contains(".")) {
                                            tmpA = tmpA.concat(".0");
                                        }
                                        Object[] obj = (Object[]) acc.get(counter.get());
                                        obj[0] = (Double) obj[0] + Double.parseDouble(tmpA);
                                        obj[1] = (Double) obj[1] + 1.0;
                                        acc.set(counter.get(), obj );
                                        break;
                                    case "count":
                                        acc.set(counter.get(), (Double) acc.get(counter.get()) + (Double) 1.0);
                                        break;
                                }
                                counter.getAndIncrement();

                            });
                                return acc;
                            }, (acc1, acc2) -> {
                                AtomicInteger counter = new AtomicInteger(0);
                                current.getJSONArray("agg_colIDs").forEach(aggColObj -> {
                                    JSONObject aggCol = (JSONObject) aggColObj;
                                    //TODO other startValues
                                    switch(aggCol.getString("type")) {
                                        case "sum":
                                            //acc.set(counter.get(), Double.parseDouble((String) acc.get(counter.get())) + Double.parseDouble((String) x.get(aggCol.getIntValue("colID"))));
                                            acc1.set(counter.get(), (Double) acc1.get(counter.get()) + (Double) acc2.get(counter.get()));
                                            break;
                                        case "average":
                                            //acc.set(counter.get(), (Double) acc.get(counter.get()));
                                            Object[] obj = (Object[]) acc1.get(counter.get());
                                            Object[] obj2 = (Object[]) acc2.get(counter.get());
                                            obj[0] = (Double) obj[0] + (Double) obj2[0];
                                            obj[1] = (Double) obj[1] + (Double) obj2[1];
                                            acc1.set(counter.get(), obj );
                                            break;
                                        case "count":
                                            acc1.set(counter.get(), (Double) acc1.get(counter.get()) + (Double) acc2.get(counter.get()));
                                            break;
                                    }
                                    counter.getAndIncrement();

                                });
                                return acc1;
                            });

                        returnRdd = pairRDD.map(row -> {
                            ArrayList<Object> newRow = new ArrayList<Object>(row._1);
                            row._2.forEach(rObj -> {
                                if (rObj.getClass().equals(Object[].class)) {
                                    //is average
                                    Object[] r = (Object[]) rObj;
                                    newRow.add((Double) r[0] / (Double) r[1]);
                                } else {
                                    newRow.add(rObj);
                                }
                            });
                            return newRow;
                        });
                        //returnRdd.aggregate()
                        break;

                    default:
                        System.err.println("Unknown Type: " + type + "!");
                }
                }

            return returnRdd;

        } catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    private static double calcOperators(JSONObject o, ArrayList<Object> row) {
        if(o.containsKey("colID")) {
            return Double.parseDouble((String) row.get(o.getIntValue("colID")));
        } else if (o.containsKey("operator")) {
            switch (o.getString("operator")) {
                case "*":
                    return calcOperators(o.getJSONObject("left"), row) * calcOperators(o.getJSONObject("right"), row);
                case "-":
                    return calcOperators(o.getJSONObject("left"), row) - calcOperators(o.getJSONObject("right"), row);
                case "+":
                    return calcOperators(o.getJSONObject("left"), row) + calcOperators(o.getJSONObject("right"), row);
                default:
                    return 0.0;
            }
        } else if (o.containsKey("value")) {
            return o.getDouble("value");
        } else {
            return 0.0;
        }
    }

}
