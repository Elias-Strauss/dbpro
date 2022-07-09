package optimizers.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class CalciteOptimizer {

    private final GenSchema schema;

    public CalciteOptimizer(String schemaPath) throws IOException {
        this.schema = new GenSchema(schemaPath);
    }

    public RelNode optimizeQuery(String sqlQuery) throws SqlParseException {

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

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

        SqlNode sqlNode = SqlQueryParser.parseString(sqlQuery);
        SqlNode validated = validator.validate(sqlNode);

        HepProgram hepProgram = new HepProgramBuilder()
                .addRuleInstance(ProjectFilterTransposeRule.Config.DEFAULT.toRule())
                .addRuleInstance(ProjectJoinTransposeRule.Config.DEFAULT.toRule())
                .addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule())
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

        hepPlanner.setRoot(root.rel);

        return hepPlanner.findBestExp();
    }

    public GenSchema getSchema() {
        return schema;
    }

}
