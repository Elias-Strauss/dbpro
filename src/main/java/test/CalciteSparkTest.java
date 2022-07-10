package test;

import optimizers.calcite.CalciteOptimizer;
import org.apache.calcite.sql.parser.SqlParseException;

import java.io.IOException;

public class CalciteSparkTest {

    public static void main (String[] args) throws IOException, SqlParseException {
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

        CalciteOptimizer calciteOptimizer = new CalciteOptimizer(schemaPath);

        System.out.println(calciteOptimizer.optimizeQuery(sqlQuery).explain());
    }
}
