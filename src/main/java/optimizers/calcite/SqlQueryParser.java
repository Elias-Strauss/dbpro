package optimizers.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlConformanceEnum;


public class SqlQueryParser
{
    public static SqlNode parseString(String SQLQuery) throws SqlParseException
    {
        SqlParser.Config config = SqlParser.Config.DEFAULT
                .withCaseSensitive(true)
                .withQuoting(Quoting.DOUBLE_QUOTE)
                .withConformance(SqlConformanceEnum.DEFAULT)
                .withQuotedCasing(Casing.TO_LOWER)
                .withUnquotedCasing(Casing.TO_LOWER);

        SqlParser sqlParser = SqlParser.create(SQLQuery, config);
        return sqlParser.parseStmt();
    }
}