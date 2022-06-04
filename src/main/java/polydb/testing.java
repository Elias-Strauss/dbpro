package polydb;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import polydb.core.queryinterface.SqlInterface;
import polydb.pipelines.PipelineTest;
import polydb.pipelines.SQLRheemPipeline;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

public class testing {
    String calciteModel = "src/main/resources/inputconfig.json";
    String query = "INSERT INTO CSV.\"sink\" SELECT \"photoId\",CLEAN_STR(\"details\") FROM CSV.\"photos\"";
    public static void main(String[] args) throws Exception {
        System.out.println("asd");
        //PipelineTest test = new PipelineTest();
        //test.execute("INSERT INTO CSV.\"sink\" SELECT \"photoId\",CLEAN_STR(\"details\") FROM CSV.\"photos\"");
        System.out.println("new rheem pipeline:");
        SQLRheemPipeline pipeline = new SQLRheemPipeline("src/main/resources/inputconfig.json");
        System.out.println("sercsvsinkpath:");
        pipeline.setCsvSinkPath("file:///tmp/polydb/sink.csv");
        System.out.println("setjdbcoutpprop:");
        pipeline.setJdbcOutputProperties("src/main/resources/jdbc_sink.properties");
        System.out.println("optimize");
        pipeline.optimize("INSERT INTO CSV.\"sink\" SELECT \"photoId\",CLEAN_STR(\"details\") FROM CSV.\"photos\"");


        /*SqlInterface sl = new SqlInterface("src/main/resources/inputconfig.json");
        SqlNode logicalPlan =sl.getLogicalPlan("INSERT INTO CSV.\"sink\" " +
                "SELECT \"p_userId\", COUNT(*) AS user_views " +
                "FROM CSV.\"photos\" " +
                "GROUP BY \"p_userId\"  ");
        System.out.println("asd");
        RelNode validatedPlan = sl.validateLogicalPlan(logicalPlan);*/
        //RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));
        /*System.out.println("validated plan: ");
        validatedPlan.explain(rw);*/
    }
}
