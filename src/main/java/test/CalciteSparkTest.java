package test;

import optimizers.calcite.CalciteOptimizer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.sql.parser.SqlParseException;

import java.io.IOException;

public class CalciteSparkTest {

    public static void main (String[] args) throws IOException, SqlParseException {
        String schemaPath = "src/main/resources/TPC-HTestDaten/CalciteSchema.json";

        String sqlQuery = "select\n" +
                "    s_acctbal,\n" +
                "    s_name,\n" +
                "    n_name,\n" +
                "    p_partkey,\n" +
                "    p_mfgr,\n" +
                "    s_address,\n" +
                "    s_phone,\n" +
                "    s_comment\n" +
                "from\n" +
                "    part,\n" +
                "    supplier,\n" +
                "    partsupp,\n" +
                "    nation,\n" +
                "    region\n" +
                "where\n" +
                "        p_partkey = ps_partkey\n" +
                "  and s_suppkey = ps_suppkey\n" +
                "  and p_size = 15\n" +
                "  and p_type like '%BRASS'\n" +
                "  and s_nationkey = n_nationkey\n" +
                "  and n_regionkey = r_regionkey\n" +
                "  and r_name = 'EUROPE'\n" +
                "  and ps_supplycost = (\n" +
                "    select\n" +
                "        min(ps_supplycost)\n" +
                "    from\n" +
                "        partsupp,\n" +
                "        supplier,\n" +
                "        nation,\n" +
                "        region\n" +
                "    where\n" +
                "            p_partkey = ps_partkey\n" +
                "      and s_suppkey = ps_suppkey\n" +
                "      and s_nationkey = n_nationkey\n" +
                "      and n_regionkey = r_regionkey\n" +
                "      and r_name = 'EUROPE'\n" +
                ")\n" +
                "order by\n" +
                "    s_acctbal desc,\n" +
                "    n_name,\n" +
                "    s_name,\n" +
                "    p_partkey";

        CalciteOptimizer calciteOptimizer = new CalciteOptimizer(schemaPath);

        RelNode optimized = calciteOptimizer.optimizeQuery(sqlQuery);

//        Sort sort = (Sort) optimized;
//
//        sort.getSortExps().forEach(exp -> {
//            System.out.println(exp);
//        });
//
//        sort.collation.getFieldCollations().forEach(scol -> {
//            System.out.println(scol.direction.toString());
//        });

        new RelVisitor(){
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

                System.out.println(node);

                super.visit(node, ordinal, parent);
            }

        }.go(optimized);




        System.out.println("------------------------------");

        System.out.println(optimized.explain());
    }
}
