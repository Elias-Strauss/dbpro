package polydb.pipelines;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Test Class to fiddle with different optimizer strategies, planners and Rules
 */
public class PipelineTest {
    public void execute(String query) throws SQLException, SqlParseException, ValidationException, RelConversionException {

        // setup
        final Properties properties = new Properties();
        properties.setProperty("model", "src/main/resources/inputconfig.json");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus schema = calciteConnection.getRootSchema().getSubSchema(connection.getSchema()); // actually type SchemaPlusImpl
        final RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(System.out, true));

        // parse query
        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                //.costFactory(new MyCostBase.MyCostFactory()) // propably wrong place for the costFactory
                .costFactory(RelOptCostImpl.FACTORY)
                .build();

        Planner planner = Frameworks.getPlanner(frameworkConfig); // will actually be of Type PlannerImpl
        final SqlNode parsed = planner.parse(query);

        // validate query
        final SqlNode sqlValidated = planner.validate(parsed);

        // transform to relNodes
        final RelNode relValidated = planner.rel(sqlValidated).project();

        RelOptCluster cluster = relValidated.getCluster();
        // cluster.setMetadataQuerySupplier(CustomRelMetadataQuery::new);


        System.out.println("Validated Plan:");
        relValidated.explain(relWriter);

        // Heuristic Optimization
        HepProgram program = HepProgram.builder()
                .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
                .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
                .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
                .addRuleInstance(LoptOptimizeJoinRule.INSTANCE) // Heuristic Rule to optimize Join Order
                .build();

        // HepPlanner hepPlanner = new HepPlanner(program);
        final HepPlanner hepPlanner = new HepPlanner(program, null, false, null, RelOptCostImpl.FACTORY);
        hepPlanner.setRoot(relValidated);
        RelNode optimizedPlan = hepPlanner.findBestExp();

        System.out.println("Heuristics Optimized:");
        optimizedPlan.explain(relWriter);


        // trying standard program with volcano planner
        final Program standard = Programs.standard();
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) cluster.getPlanner();
        RelTraitSet requiredOutputTraits = relValidated.getTraitSet();
        List<RelOptMaterialization> emptyMaterializations = new ArrayList<>();
        List<RelOptLattice> emptyLattices = new ArrayList<>();


        final Program heuristicJoinOrder = Programs.heuristicJoinOrder(Programs.RULE_SET, false, 0);

        final RelNode hepOptimized = heuristicJoinOrder.run(hepPlanner,
                relValidated,
                requiredOutputTraits,
                emptyMaterializations,
                emptyLattices
        );

        System.out.println("heuristic Join Order Optimized");
        hepOptimized.explain(relWriter);

        /*
        standard.run(volcanoPlanner,
                relValidated,
                requOutTraits,
                emptyMaterializations,
                emptyLattices);

         */
        /*
        // Volcano Planner as in CboQueryExampleSnippet
        //cluster.setMetadataQuerySupplier(CustomRelMetadataQuery::new);
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) cluster.getPlanner();
        RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
        RelNode newRoot = volcanoPlanner.changeTraits(relValidated, desiredTraits);
        volcanoPlanner.setRoot(newRoot);
        RelNode volcanoOptimized = volcanoPlanner.findBestExp();


        System.out.println("Volcano Planner Optimized:");
        volcanoOptimized.explain(relWriter);

         */

    }

    public void exec2(SqlNode parsed) {

        Properties configProperties = new Properties();

        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);


        // Pipeline I found online
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();


        VolcanoPlanner planner1 = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config)
        );

        planner1.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster1 = RelOptCluster.create(
                planner1,
                new RexBuilder(typeFactory)
        );

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(true)
                .withExpand(false)
                .build();

        /*
        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );

         */

    }
}
