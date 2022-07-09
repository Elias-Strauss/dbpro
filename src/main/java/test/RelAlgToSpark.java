package test;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

                    System.out.println("scan");
                    System.out.println(TableScan);
                    System.out.println(node);
                }

                if (node instanceof TableModify) {

                }
                //TODO: handle the order of the projected columns
                if (node instanceof Project) {
                    /*System.out.println(node);
                    System.out.println("project "+counter);
                    System.out.println(node.getRowType().getFieldNames());
                    System.out.println(node.getInput(0).getRowType().getFieldNames());*/
                    Project projnode2 = (Project) node;
                    JSONObject Project = new JSONObject();
                    Project.put("type","Project");
                    Project.put("id",String.valueOf(node.getId()));
                    JSONArray colIDs = new JSONArray();
                    //List<RexNode> childExps2 = projnode2.getChildExps();
                    List<RexNode> childExps2 = projnode2.getProjects();
                    for (RexNode r : childExps2){
                        /*System.out.println("child");
                        System.out.println(r.toString());*/
                        if(r.toString().substring(0,1).equals("$"))
                        colIDs.put(r.toString().replace("$",""));
                        else{


                            RexCall rexCall = (RexCall) r;
                            colIDs.put(recTrav(rexCall));
                            /*System.out.println(rexCall.operands);
                            System.out.println(rexCall.op);
                            System.out.println(rexCall.operands.get(0));
                            System.out.println(rexCall.operands.get(1));
                            RexCall rexCall1 = (RexCall) rexCall.getOperands().get(1);
                            System.out.println(rexCall1.getOperands());*/
                            /*for (RexNode rn : rexCall.getOperands()) {
                                System.out.println(rn);
                            }*/
                        }
                    }
                    Project.put("colIDs",colIDs);
                    Project.put("id_a",String.valueOf(projnode2.getInput().getId()));
                    operations.put(Project);
                    System.out.println(Project);

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
                    Filter.put("condition",((RexCall) cond).op.toString());
                    List<RexNode> operands = ((RexCall) cond).getOperands();
                    /*System.out.println(((RexCall) cond).op.toString());
                    System.out.println(operands);
                    System.out.println(operands.get(1).toString().split(":")[1].substring(0,8));*/
                    if(operands.get(1).toString().split(":")[1].substring(0,8).equals("INTERVAL")){
                        Filter.put("condType","Date");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String raw = operands.get(1).toString().split(":")[0];
                        String operator = raw.substring(0,1);
                        String date = raw.substring(2,12);
                        String operand = raw.substring(14);
                        Date date1;
                        try {
                            date1 = sdf.parse(date);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                        long millis = date1.getTime();
                        long operand1 = Long.valueOf(operand);
                        long result=0;
                        if (operator.equals("-")) result=millis-operand1;
                        if (operator.equals("+")) result=millis+operand1;
                        Date date2 = new Date(result);
                        Filter.put("date",sdf.format(date2));
                        //System.out.println(operands.get(1).toString().split(":")[0]);
                    }else{
                        Filter.put("conditionVal",operands.get(1).toString().split(":")[0]);
                    }

                    Filter.put("colID",operands.get(0).toString().replace("$", ""));

                    Filter.put("id_a",String.valueOf(filter.getInput().getId()));
                    //System.out.println(Filter);
                    operations.put(Filter);
                }

                if (node instanceof Join) {
                    System.out.println("join");
                    System.out.println(node);
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
                    //System.out.println(node);
                    //System.out.println(sortNode.getSortExps());
                    //System.out.println(sortNode.);
                    JSONArray colIDs = new JSONArray();
                    for(RexNode r:sortNode.getSortExps()){
                        colIDs.put(r.toString().replace("$", ""));
                        //r.
                    }
                    String[] split = sortNode.toString().split("dir");
                    JSONArray orders = new JSONArray();
                    for(int i=1;i<split.length;i++){
                        //System.out.println(split[i]);
                        if(split[i].replace("ASC","").length()<split[i].length()){
                            orders.put("ASC");
                        }else{
                            orders.put("DESC");
                        }
                    }
                    //int sortKey = Integer.parseInt(sortNode.getChildExps().get(0).toString().replace("$", ""));
                    /*int sortKey = Integer.parseInt(sortNode.getSortExps().get(0).toString().replace("$", ""));
                    int posStart=sortNode.toString().indexOf("dir0")+5;
                    String order = sortNode.toString().substring(posStart,sortNode.toString().length()-1);*/

                    JSONObject Sort = new JSONObject();
                    Sort.put("type","Sort");
                    Sort.put("id",String.valueOf(node.getId()));
                    //Sort.put("sortKey",String.valueOf(sortKey));
                    //Sort.put("order",order);
                    Sort.put("id_a",String.valueOf(sortNode.getInput().getId()));
                    Sort.put("colIDs",colIDs);
                    Sort.put("orders",orders);

                    operations.put(Sort);
                    System.out.println(Sort);

                }

                if (node instanceof Aggregate) {
                    Aggregate aggregate = (Aggregate) node;
                    System.out.println("aggregate");
                    System.out.println(node);
                    JSONObject Aggregate = new JSONObject();
                    Aggregate.put("type","Aggregate");
                    Aggregate.put("id",String.valueOf(node.getId()));
                    Aggregate.put("id_a",String.valueOf(aggregate.getInput().getId()));
                    //System.out.println(node.getRowType().getFieldNames());
                    //System.out.println(parent.getRowType().getFieldNames());
                    //System.out.println(aggregate.getGroupSet());
                    JSONArray groupCols = new JSONArray();
                    for (int i : aggregate.getGroupSet()){
                        groupCols.put(String.valueOf(i));
                    }
                    Aggregate.put("groupCols",groupCols);
                    JSONArray aggrs = new JSONArray();
                    for(AggregateCall ac : aggregate.getAggCallList()){
                        JSONObject agg = new JSONObject();
                        /*System.out.println(ac);
                        System.out.println(ac.getArgList());*/
                        agg.put("type",ac.getAggregation().toString());
                        if(!(ac.getAggregation().toString().equals("COUNT")))agg.put("colID",ac.getArgList().get(0));
                        aggrs.put(agg);
                    }
                    Aggregate.put("aggregations",aggrs);
                    operations.put(Aggregate);
                    System.out.println(Aggregate);
                }



                super.visit(node, ordinal, parent);
            }
        }.
                go(optimizedPlan);

        return operations;
    }

    private JSONObject recTrav(RexCall rx){
        JSONObject opTree = new JSONObject();
        opTree.put("operator",rx.op.toString());
        /*System.out.println("TUTAJ");
        System.out.println(rx.operands.get(0).toString());*/
        String first = rx.operands.get(0).toString().substring(0,1);
        if(!(first.equals("*") || first.equals("+") || first.equals("-"))){
            opTree.put("left",rx.operands.get(0).toString().replace("$", ""));
        /*}else if(rx.op.toString().equals("*") && rx.operands.get(0).toString().length()==2){
            opTree.put("left",rx.operands.get(0).toString().replace("$", ""));
        */}else{
            RexNode left = rx.operands.get(0);
            RexCall rexCall = (RexCall) left;
            opTree.put("left",recTrav(rexCall));
        }
        first = rx.operands.get(1).toString().substring(0,1);
        if(!(first.equals("*") || first.equals("+") || first.equals("-"))){
            opTree.put("right",rx.operands.get(1).toString().replace("$", ""));
        }else{
            RexNode right = rx.operands.get(1);
            RexCall rexCall = (RexCall) right;
            opTree.put("right",recTrav(rexCall));
        }

        return opTree;
    }

}
