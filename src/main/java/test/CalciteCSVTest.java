package test;

import optimizers.calcite.GenSchema;
import optimizers.calcite.SqlQueryParser;
import optimizers.calcite.GenTable;
import optimizers.calcite.GenTableStatistic;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CalciteCSVTest {

    public static void main (String[] strg) throws SqlParseException, ValidationException, RelConversionException, IOException {
        File CsvDirectory = new File("src/main/resources/testData");
        //CsvSchema csvSchema = new CsvSchema(CsvDirectory, CsvTable.Flavor.SCANNABLE);
        //System.out.println(csvSchema.getTable("actor"));

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

//        JSONParser jsonParser = new JSONParser();
//
//        Reader reader = new FileReader("src/main/resources/TPC-HTestDaten/CalciteSchema.json");
//
//        Object jsonObj = jsonParser.parse(reader);
//
//        JSONObject jsonObject = (JSONObject) jsonObj;
//
//        System.out.println(jsonObject.get("Tables"));
//
//        JSONArray Tables = (JSONArray) jsonObject.get("Tables");
//
//        System.out.println(Tables.get(0));

        GenSchema schema = new GenSchema("src/main/resources/TPC-HTestDaten/CalciteSchema.json");

//        GenTable actorTable = new GenTable(
//                "lineitem",
//                List.of("orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receipdate", "shipinstruct", "shipmode", "comment"),
//                List.of(SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.DECIMAL, SqlTypeName.DECIMAL, SqlTypeName.DECIMAL, SqlTypeName.DECIMAL, SqlTypeName.CHAR, SqlTypeName.CHAR, SqlTypeName.DATE, SqlTypeName.DATE, SqlTypeName.DATE, SqlTypeName.DATE, SqlTypeName.CHAR, SqlTypeName.CHAR, SqlTypeName.CHAR),
//                new GenTableStatistic(200));
//        GenSchema test = new GenSchema("Test", Map.of("lineitem", actorTable));

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(schema.getSchemaName()),
                typeFactory,
                config
        );

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                validatorConfig
        );


        String query = "select\n" +
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

        System.out.println("\u001B[31m" + "------------SQL input------------" + "\u001B[0m");
        System.out.println(query);
        System.out.println("\n" + "\u001B[31m" + "------------Parsed SQL statement------------" + "\u001B[0m");
        SqlNode sqlNode = SqlQueryParser.parseString(query);
        SqlNode validated = validator.validate(sqlNode);
        System.out.println(validated.toString());

        VolcanoPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config)
        );

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        HepProgram hepProgram = new HepProgramBuilder()
                .addRuleInstance(ProjectFilterTransposeRule.Config.DEFAULT.toRule())
                .addRuleInstance(ProjectJoinTransposeRule.Config.DEFAULT.toRule())
                .addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule())
                .addRuleCollection(EnumerableRules.rules())
                .build();

        HepPlanner hepPlanner = new HepPlanner(hepProgram);

        RelOptCluster cluster = RelOptCluster.create(
                hepPlanner,
                new RexBuilder(typeFactory)
        );

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.CONFIG
                .withTrimUnusedFields(true)
                .withExpand(false);
        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );

        RelRoot root = converter.convertQuery(validated, false, true);

        Program program = Programs.of(RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE
                ));

        hepPlanner.setRoot(root.rel);


        System.out.println("\n" + "\u001B[31m" + "------------Relational pLan------------" + "\u001B[0m");
        System.out.println(root.rel.explain());
        System.out.println("\u001B[31m" + "------------Optimized plan------------" + "\u001B[0m");
        RelNode test = hepPlanner.findBestExp();
        System.out.println(test.explain());
        hepPlanner.changeTraits(test, test.getTraitSet().plus(EnumerableConvention.INSTANCE));

        System.out.println(hepPlanner.findBestExp().explain());



//        program.run(
//                planner,
//                root.rel,
//                RelTraitSet.createEmpty(),
//                Collections.emptyList(),
//                Collections.emptyList()
//        );

//        // Ponasanje planner-a definira se konfiguracijom. U njoj
//        // se definiraju rulovi, sheme i sve ostalo.
//        Planner planner = Frameworks.getPlanner(config);
//
//        //SqlNode sqlNode = planner.parse(new SourceStringReader("SELECT * FROM dvdrental.\"actor\" as a1,dvdrental.\"film_actor\" as a2 WHERE a1.\"actor_id\"=a2.\"film_id\" "));
//        SqlNode sqlNode = planner.parse(new SourceStringReader("SELECT * FROM \"actor\""));
//        //SQLparser parser = new SQLparser();
//        //SqlNode sqlNode = parser.getParsed("SELECT * FROM actor");
//
//        System.out.println(sqlNode.toString());
//
//        sqlNode = planner.validate(sqlNode);
//
//        RelRoot relRoot = planner.rel(sqlNode);
//        System.out.println(relRoot.toString());
//        RelWriter rw = new RelWriterImpl(new PrintWriter(System.out, true));
//
//        RelNode relNode = relRoot.project();
//        System.out.println("Validated Plan:");
//        relNode.explain(rw);
//
//
//        RelAlgOptimizer qo = new RelAlgOptimizer();
//
//        RelNode relNodeOptimized=qo.optimizePlan(relNode);
//        System.out.println("Optimized Plan:");
//        relNodeOptimized.explain(rw);
//
//        RelAlgToSpark qt = new RelAlgToSpark();
//        qt.translatePlan(relNodeOptimized);
    }
}
