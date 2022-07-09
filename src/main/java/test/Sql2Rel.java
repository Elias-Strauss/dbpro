package test;

import optimizers.calcite.CalciteOptimizer;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.jdbc.CalciteConnection;

import org.apache.calcite.schema.SchemaPlus;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelNode;

import org.apache.calcite.util.SourceStringReader;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.Connection;
import java.util.Properties;
import javax.sql.DataSource;
import org.postgresql.Driver;

import org.apache.calcite.adapter.jdbc.JdbcSchema;

import org.apache.calcite.plan.RelOptUtil;
import org.json.JSONArray;
import org.json.JSONObject;

// Ovaj SQL upit radi: 'SELECT * FROM multidb."medinfo"'

public class Sql2Rel
{
    public static void main( String[] args ) throws Exception
    {
        String schemaPath = "src/main/resources/TPC-HTestDaten/CalciteSchema.json";

        String sqlQuery = "select\n" +
                "\tl_returnflag,\n" +
                "\tl_linestatus,\n" +
                "\tsum(l_quantity) as sum_qty,\n" +
                "\tsum(l_extendedprice) as sum_base_price,\n" +
                "\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
                "\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
                "\tavg(l_quantity) as avg_qty,\n" +
                "\tavg(l_extendedprice) as avg_price,\n" +
                "\tavg(l_discount) as avg_disc,\n" +
                "\tcount(*) as count_order\n" +
                "from\n" +
                "\tlineitem\n" +
                "where\n" +
                "\tl_shipdate <= date '1998-12-01' - interval '90' day\n" +
                "group by\n" +
                "\tl_returnflag,\n" +
                "\tl_linestatus\n" +
                "order by\n" +
                "\tl_returnflag,\n" +
                "\tl_linestatus";

        /*String sqlQuery = "select\n" +
                "        s_acctbal,\n" +
                "        s_name,\n" +
                "        n_name,\n" +
                "        p_partkey,\n" +
                "        p_mfgr,\n" +
                "        s_address,\n" +
                "        s_phone,\n" +
                "        s_comment\n" +
                "from\n" +
                "        part,\n" +
                "        supplier,\n" +
                "        partsupp,\n" +
                "        nation,\n" +
                "        region\n" +
                "where\n" +
                "        p_partkey = ps_partkey\n" +
                "        and s_suppkey = ps_suppkey\n" +
                "        and p_size = 15\n" +
                "        and p_type like '%BRASS'\n" +
                "        and s_nationkey = n_nationkey\n" +
                "        and n_regionkey = r_regionkey\n" +
                "        and r_name = 'EUROPE'\n" +
                "        and ps_supplycost = (\n" +
                "                select\n" +
                "                        min(ps_supplycost)\n" +
                "                from\n" +
                "                        partsupp,\n" +
                "                        supplier,\n" +
                "                        nation,\n" +
                "                        region\n" +
                "                where\n" +
                "                        p_partkey = ps_partkey\n" +
                "                        and s_suppkey = ps_suppkey\n" +
                "                        and s_nationkey = n_nationkey\n" +
                "                        and n_regionkey = r_regionkey\n" +
                "                        and r_name = 'EUROPE'\n" +
                "        )\n" +
                "order by\n" +
                "        s_acctbal desc,\n" +
                "        n_name,\n" +
                "        s_name,\n" +
                "        p_partkey\n" +
                "limit 100";*/

        CalciteOptimizer calciteOptimizer = new CalciteOptimizer(schemaPath);

        RelNode relNode = calciteOptimizer.optimizeQuery(sqlQuery);
        System.out.println("Optimized Plan:");
        System.out.println(relNode.explain());



        RelAlgToSpark qt = new RelAlgToSpark();
        JSONArray output =qt.translatePlan(relNode);
        System.out.println(output);
        JSONArray outputInversed = new JSONArray();
        for (Object o : output){
            System.out.println(o);
            JSONObject temp = (JSONObject) o;
            outputInversed.put(temp);
        }
        FileWriter file = new FileWriter("src/main/resources/output.json");
        file.write(outputInversed.toString());
        file.close();
    }
}