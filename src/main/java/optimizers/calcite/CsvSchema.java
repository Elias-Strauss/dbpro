package optimizers.calcite;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class CsvSchema extends AbstractSchema {

    private final String name;
    private final Map<String, Table> tableMap;

    public CsvSchema(String name, Map<String, Table> tableMap) {
        this.name = name;
        this.tableMap = tableMap;
    }



    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    public String getSchemaName() {
        return this.name;
    }
}
