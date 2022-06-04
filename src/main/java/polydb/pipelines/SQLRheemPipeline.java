package polydb.pipelines;

import polydb.core.optimization.RelAlgOptimizer;
import polydb.core.queryinterface.SqlInterface;
import polydb.metadata.MetadataHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlNode;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQLRheemPipeline resembles a PolyDB Pipeline, which accepts SQL queries, optimizes based on relational algebra,
 * translates the optimized relational algebra plan to a Rheem plan, and finally executes it on Rheem
 */
public class SQLRheemPipeline {

    public MetadataHandler mh;
    public SqlInterface qi;
    public RelAlgOptimizer qo;
    //public RelAlgToRheemTranslator qt;
    //public RheemExecutor qe;
    public Properties jdbcSinkProps;
    public String csvSinkPath;
    public boolean hasSink;

    public RelNode validatedPlan;
    public RelNode optimizedPlan;
    //public RheemPlan translatedPlan;


    public SQLRheemPipeline(String inputConfig) throws IOException {
        initialize(inputConfig);
    }


    public void initialize(String inputConfig) throws IOException {
        this.mh = new MetadataHandler();
        this.qi = new SqlInterface(inputConfig);
        this.qo = new RelAlgOptimizer();
        //this.qt = new RelAlgToRheemTranslator(mh);
        //this.qe = new RheemExecutor();
        this.hasSink = false;
        this.jdbcSinkProps = null;
        this.csvSinkPath = null;
    }

    public void registerUdf(ScalarUdf udf) throws SQLException {
        qi.registerUdf(udf);
    }

    public void setCsvSinkPath(String csvSinkPath) {
        this.csvSinkPath = csvSinkPath;
        this.hasSink = true;
    }

    public void setJdbcOutputProperties(String jdbcSinkPropsPath) {
        try (InputStream input = new FileInputStream(jdbcSinkPropsPath)) {
            this.jdbcSinkProps = new Properties();
            this.jdbcSinkProps.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        this.hasSink = true;
    }

    public void optimize(String query) throws Exception {
        if (!this.hasSink) {
            throw new Exception("Please specify a sink for this pipeline");
        }

        //register HBase tables to Hive
        if (query.contains("HBASE.")) {
            Pattern pattern = Pattern.compile("HBASE[.](.*?)[ ,]", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(query);
            while (matcher.find()) {
                String table = matcher.group(1).replace("\"", "");
                mh.move("hbase", "default", table, "hive", "default", table);
            }

            query = query.replace("HBASE.", "HIVE.").replace("cf:", "");

        }

        RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));

        SqlNode logicalPlan = qi.getLogicalPlan(query);
        this.validatedPlan = qi.validateLogicalPlan(logicalPlan);
        System.out.println("Validated Plan:");
        this.validatedPlan.explain(rw);

        //optimize query
        this.optimizedPlan = this.qo.optimizePlan(validatedPlan);

        //print optimized plan
        System.out.println("Optimized Plan:");
        this.optimizedPlan.explain(rw);

        //translate query to executable plan
        /*if (csvSinkPath != null)
            qt.setCsvSinkPath(csvSinkPath);
        if (jdbcSinkProps != null)
            qt.setJdbcSinkProps(jdbcSinkProps);

        System.out.println("\nExecutable Plan:");
        this.translatedPlan = qt.translatePlan(optimizedPlan);*/
    }

    /*public void execute() throws Exception {

        this.qe.execute(this.translatedPlan, mh);
    }*/
}
