package calcite.test;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * Hello world!
 *
 */
public class SQLparser
{
    public static void main( String[] args ) throws SqlParseException
    {
        SqlParser sqlParser = SqlParser.create(new SourceStringReader("SELECT * from captians join streets where streets.name = test"),
                new SqlParser.Config() {
                    @Override
                    public SqlParser.Config withIdentifierMaxLength(int i) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withQuotedCasing(Casing casing) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withUnquotedCasing(Casing casing) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withQuoting(Quoting quoting) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withCaseSensitive(boolean b) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withConformance(SqlConformance sqlConformance) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withCharLiteralStyles(Iterable<CharLiteralStyle> iterable) {
                        return null;
                    }

                    @Override
                    public SqlParser.Config withParserFactory(SqlParserImplFactory sqlParserImplFactory) {
                        return null;
                    }
                });
//                }SqlParser.Config()
//                        .setParserFactory(SqlParserImpl.FACTORY)
//                        .setQuoting(Quoting.DOUBLE_QUOTE)
//                        .setUnquotedCasing(Casing.TO_UPPER)
//                        .setQuotedCasing(Casing.UNCHANGED)
//                        .setConformance(SqlConformanceEnum.DEFAULT)
//                        .build());
        SqlNode sqlNode = sqlParser.parseQuery();
        System.out.println(sqlNode.toString());
    }
}