package calcite.test;

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

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.Connection;
import java.util.Properties;
import javax.sql.DataSource;
//import org.postgresql.Driver;

import org.apache.calcite.adapter.jdbc.JdbcSchema;

import org.apache.calcite.plan.RelOptUtil;

// Ovaj SQL upit radi: 'SELECT * FROM multidb."medinfo"'

public class Sql2Rel
{
    public static void main( String[] args ) throws Exception
    {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        //TODO: do not push the password !!
        final DataSource ds = JdbcSchema.dataSource(
                "jdbc:postgresql://localhost:5432/dvdrental",
                "org.postgresql.Driver",
                "postgres",
                "admin");
        rootSchema.add("DVDRENTAL", JdbcSchema.create(rootSchema, "DVDRENTAL", ds, null, null));
        System.out.println(rootSchema.toString());

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        // Ponasanje planner-a definira se konfiguracijom. U njoj
        // se definiraju rulovi, sheme i sve ostalo.
        Planner planner = Frameworks.getPlanner(config);

        //SqlNode sqlNode = planner.parse(new SourceStringReader("SELECT * FROM dvdrental.\"actor\" as a1,dvdrental.\"film_actor\" as a2 WHERE a1.\"actor_id\"=a2.\"film_id\" "));
        SqlNode sqlNode = planner.parse(new SourceStringReader("SELECT \"C\"." +
                "\"customer_id\",\"P\".\"customer_id\",\"P\".\"amount\"," +
                "\"P\".\"payment_id\" FROM dvdrental.\"payment\" as p, dvdrental." +
                "\"customer\" as c WHERE \"P\".\"amount\">5" +
                " and \"P\".\"customer_id\"=\"C\".\"customer_id\" group by " +
                "\"P\".\"customer_id\",\"C\".\"customer_id\",\"P\".\"amount\",\"P\".\"payment_id\" " +
                "order by \"P\".\"amount\" asc"));
        //SQLparser parser = new SQLparser();
        //SqlNode sqlNode = parser.getParsed("SELECT * FROM actor");

        System.out.println(sqlNode.toString());

        sqlNode = planner.validate(sqlNode);

        RelRoot relRoot = planner.rel(sqlNode);
        System.out.println(relRoot.toString());
        RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

        RelNode relNode = relRoot.project();
        System.out.println("Validated Plan:");
        relNode.explain(rw);


        RelAlgOptimizer qo = new RelAlgOptimizer();

        RelNode relNodeOptimized=qo.optimizePlan(relNode);
        System.out.println("Optimized Plan:");
        relNodeOptimized.explain(rw);

        RelAlgToSpark qt = new RelAlgToSpark();
        qt.translatePlan(relNodeOptimized);
    }
}