package calcite.test;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
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
    public SqlNode getParsed( String args ) throws SqlParseException
    {
        SqlParser sqlParser = SqlParser.create(new SourceStringReader(args),
                SqlParser.configBuilder()
                        .setParserFactory(SqlParserImpl.FACTORY)
                        .setQuoting(Quoting.DOUBLE_QUOTE)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setConformance(SqlConformanceEnum.DEFAULT)
                        .build());
        return sqlParser.parseQuery();
        //System.out.println(sqlNode.toString());
    }
}