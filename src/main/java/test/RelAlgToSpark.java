package test;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.List;

public class RelAlgToSpark {

    //SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    //JavaSparkContext sc = new JavaSparkContext(conf);

    public JSONArray translatePlan(RelNode optimizedPlan){

        //List<Pair<JavaRDD<String>,String>> scanList = new ArrayList<Pair<JavaRDD<String>,String>>();
        JSONArray operations = new JSONArray();

        new RelVisitor(){


            int counter = 0;
            public RelNode go(RelNode p) {
                try {
                    visit(p, 0, null);
                } catch (Exception e) {
                    // Rewriting cannot be performed
                    System.out.println(e.getMessage());
                }
                return p;
            }
            public void visit(final RelNode node, final int ordinal, final RelNode parent) {
                counter++;
                //System.out.println("pass");

                if (node instanceof TableScan) {
                    String name =node.getTable().getQualifiedName().get(1).toLowerCase();
                    //JavaRDD<String> distFile = sc.textFile("src/main/resources/"+name+".csv").map(a->a);
                    //System.out.println(distFile.collect());
                    JSONObject TableScan = new JSONObject();
                    TableScan.put("type","TableScan");
                    TableScan.put("TableName",name);
                    TableScan.put("id",String.valueOf(node.getId()));
                    operations.put(TableScan);

                    /*System.out.println("scan");
                    System.out.println(counter);
                    System.out.println(node);*/
                }

                if (node instanceof TableModify) {

                }
                //TODO: handle the order of the projected columns
                if (node instanceof Project) {
                    //System.out.println("project "+counter);
                    //System.out.println(node.getRowType().getFieldNames());
                    //System.out.println(node.getInput(0).getRowType().getFieldNames());
                    Project projnode2 = (Project) node;
                    JSONObject Project = new JSONObject();
                    Project.put("type","Project");
                    Project.put("id",String.valueOf(node.getId()));
                    JSONArray colIDs = new JSONArray();
                    //List<RexNode> childExps2 = projnode2.getChildExps();
                    List<RexNode> childExps2 = projnode2.getProjects();
                    for (RexNode r : childExps2){
                        //System.out.println(r.toString());
                        colIDs.put(r.toString().replace("$",""));
                    }
                    Project.put("colIDs",colIDs);
                    Project.put("id_a",String.valueOf(projnode2.getInput().getId()));
                    operations.put(Project);

                    //System.out.println(node.);
                }

                //TODO: filter condition "AND"/"OR", fix condition <=
                if (node instanceof Filter) {
                    /*System.out.println("filter "+counter);
                    System.out.println(node);
                    System.out.println(node.getInput(0).getRowType().getFieldNames());*/
                    Filter filter = (Filter) node;
                    JSONObject Filter = new JSONObject();
                    Filter.put("type","Filter");
                    Filter.put("id",String.valueOf(node.getId()));
                    RexNode cond = filter.getCondition();
                    Filter.put("condition",cond.toString().substring(0,1));
                    List<RexNode> operands = ((RexCall) cond).getOperands();
                    Filter.put("colID",operands.get(0).toString().replace("$", ""));
                    Filter.put("conditionVal",operands.get(1).toString().split(":")[0]);
                    Filter.put("id_a",String.valueOf(filter.getInput().getId()));
                    operations.put(Filter);
                }

                if (node instanceof Join) {
                    //System.out.println(node);
                    JSONObject Join = new JSONObject();
                    Join.put("type","Join");
                    Join.put("id",String.valueOf(node.getId()));

                    Join join = (Join) node;
                    Join.put("id_a",String.valueOf(join.getLeft().getId()));
                    Join.put("id_b",String.valueOf(join.getRight().getId()));
                    Join.put("joinType",join.getJoinType().toString());
                    RexNode cond = join.getCondition();
                    List rc = ((RexCall) cond).getOperands();
                    int indexDol=rc.get(0).toString().indexOf("$");
                    String fst = String.valueOf(rc.get(0).toString().charAt(indexDol+1));
                    int indexDol2=rc.get(1).toString().indexOf("$");
                    String snd = String.valueOf(rc.get(1).toString().charAt(indexDol2+1));

                    String nameLeft = node.getRowType().getFieldNames().get(Integer.parseInt(fst));
                    String nameRight = node.getRowType().getFieldNames().get(Integer.parseInt(snd));
                    String joinIDleft = String.valueOf(join.getLeft().getRowType().getFieldNames().indexOf(nameLeft));
                    String joinIDright = String.valueOf(join.getRight().getRowType().getFieldNames().indexOf(nameRight));
                    Join.put("colIDLeft",joinIDleft);
                    Join.put("colIDRight",joinIDright);
                    operations.put(Join);


                }

                if (node instanceof Sort) {
                    //System.out.println("sort");
                    Sort sortNode = (Sort) node;
                    //int sortKey = Integer.parseInt(sortNode.getChildExps().get(0).toString().replace("$", ""));
                    int sortKey = Integer.parseInt(sortNode.getSortExps().get(0).toString().replace("$", ""));
                    int posStart=sortNode.toString().indexOf("dir0")+5;
                    String order = sortNode.toString().substring(posStart,sortNode.toString().length()-1);

                    JSONObject Sort = new JSONObject();
                    Sort.put("type","Sort");
                    Sort.put("id",String.valueOf(node.getId()));
                    Sort.put("sortKey",String.valueOf(sortKey));
                    Sort.put("order",order);
                    Sort.put("id_a",String.valueOf(sortNode.getInput().getId()));
                    operations.put(Sort);


                }

                if (node instanceof Aggregate) {
                    Aggregate aggregate = (Aggregate) node;
                    System.out.println("aggregate");
                    //System.out.println(node);
                    //System.out.println(node.getRowType().getFieldNames());
                    //System.out.println(parent.getRowType().getFieldNames());

                }



                super.visit(node, ordinal, parent);
            }
        }.
                go(optimizedPlan);

        return operations;
    }

}
