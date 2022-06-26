package test;

import optimizers.calcite.CsvSchema;
import optimizers.calcite.SqlQueryParser;
import optimizers.calcite.CsvTable;
import optimizers.calcite.CsvTableStatistic;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
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
import org.apache.commons.codec.language.bm.Rule;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CalciteCSVTest {

    public static void main (String[] strg) throws SqlParseException, ValidationException, RelConversionException {
        File CsvDirectory = new File("src/main/resources/testData");
        //CsvSchema csvSchema = new CsvSchema(CsvDirectory, CsvTable.Flavor.SCANNABLE);
        //System.out.println(csvSchema.getTable("actor"));

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        CsvTable actorTable = new CsvTable(
                "actor",
                List.of("id", "name", "city", "dateCreated"),
                List.of(SqlTypeName.INTEGER, SqlTypeName.CHAR, SqlTypeName.CHAR, SqlTypeName.TIME),
                new CsvTableStatistic(200));
        CsvSchema schema = new CsvSchema("Test", Map.of("actor", actorTable));

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


        SqlNode sqlNode = SqlQueryParser.parseString("SELECT * FROM actor");
        System.out.println(sqlNode.toString());
        SqlNode validated = validator.validate(sqlNode);
        System.out.println(validated.toString());

        VolcanoPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config)
        );

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(
                planner,
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

        System.out.println(root.toString());

        Program program = Programs.of(RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE
                ));

        program.run(
                planner,
                root.rel,
                RelTraitSet.createEmpty(),
                Collections.emptyList(),
                Collections.emptyList()
        );

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
