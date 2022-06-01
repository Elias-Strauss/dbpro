package polydb.core.queryinterface;

import polydb.core.optimization.MyCostBase;
import polydb.pipelines.ScalarUdf;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


/**
 * The SqlInterface is an implementation of the QueryInterface, that accepts SQL queries.
 * Given an SQL statement, it converts it to a logical plan, which is then validated against the given schemata.
 */
public class SqlInterface implements QueryInterface<SqlNode, RelNode> {

    private SchemaPlus schema;
    private CalciteConnection calciteConnection;
    private FrameworkConfig config;
    private Planner planner;
    private Connection connection;

    public SqlInterface(String calciteModel) {
        setupConnection(calciteModel);
        this.planner = Frameworks.getPlanner(this.config);

    }

    public void setupConnection(String calciteModel) {

        try {

            final Properties properties = new Properties();
            properties.setProperty("model", calciteModel);

            this.connection = DriverManager.getConnection("jdbc:calcite:", properties);
            this.calciteConnection = connection.unwrap(CalciteConnection.class);

            this.schema = calciteConnection.getRootSchema().getSubSchema(connection.getSchema());
            this.config = Frameworks.newConfigBuilder()
                    .defaultSchema(this.schema)
                    .costFactory(new MyCostBase.MyCostFactory())  // TODO: maybe change this back
                    .build();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    @Override
    public SqlNode getLogicalPlan(String queryStr) {

        SqlNode sqlNode = null;
        try {
            sqlNode = planner.parse(queryStr);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        return sqlNode;
    }

    @Override
    public RelNode validateLogicalPlan(SqlNode logicalPlan) {

        /*
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        // trying to adapt planner so row-counts are preserved
        final VolcanoPlanner volcanoPlanner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config)
        );
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(
                volcanoPlanner,
                new RexBuilder(typeFactory)
        );
         */


        RelNode validatedPlan = null;
        try {
            SqlNode validate = planner.validate(logicalPlan);
            validatedPlan = planner.rel(validate).project();
            //validatedPlan = planner.rel(validate).rel;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return validatedPlan;
    }

    public void registerUdf(ScalarUdf udf) throws SQLException {
        //add UDFs
        final String functionName = udf.getName();
        final ScalarFunction cleanStrFunction = ScalarFunctionImpl.create(Types.lookupMethod(udf.getClazz(), "eval", udf.getArgTypes()));
        this.calciteConnection.getRootSchema().getSubSchema(connection.getSchema()).add(functionName, cleanStrFunction);

    }
}
