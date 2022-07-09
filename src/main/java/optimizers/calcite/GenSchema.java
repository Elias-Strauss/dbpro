package optimizers.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.Reader;
import java.util.*;

public class GenSchema extends AbstractSchema {

    private final String name;
    private final Map<String, Table> tableMap;

    public GenSchema(String name, Map<String, Table> tableMap) {
        this.name = name;
        this.tableMap = tableMap;
    }

    public GenSchema(String fileLocation) {
        JSONParser parser = new JSONParser();
        String tmpName = null;
        Map<String, Table> tmpTableMap = new HashMap<>();

        try {
            Reader reader = new FileReader(fileLocation);

            JSONObject schema = (JSONObject) parser.parse(reader);
            JSONArray Tables = (JSONArray) schema.get("Tables");

            tmpName = schema.get("SchemaName").toString();

            Tables.stream()
                    .filter(JSONObject.class::isInstance)
                    .forEach(ObjectTable -> {
                        JSONObject Table = (JSONObject) ObjectTable;

                        String genTableName;
                        List<String> genTableFieldNames = new ArrayList<>();
                        List<SqlTypeName> genTableFieldTypes = new ArrayList<>();
                        final GenTableStatistic genTableStatistic;


                        genTableName = Table.get("Name").toString();
                        genTableStatistic = new GenTableStatistic (Long.parseLong(((JSONObject) Table.get("Statistics")).get("rowCount").toString()));

                        for (Object fieldsObj : ((JSONArray) Table.get("Fields"))) {
                            JSONObject field = (JSONObject) fieldsObj;

                            genTableFieldNames.add(field.get("Name").toString());
                            genTableFieldTypes.add(this.stringToSqlTypeName(field.get("Type").toString()));
                        }
                        tmpTableMap.put(genTableName, new GenTable(genTableName, genTableFieldNames, genTableFieldTypes, genTableStatistic));
                    });

        } catch (Exception e) {
            System.err.println(e);
        }

        this.name = tmpName;
        this.tableMap = tmpTableMap;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    public String getSchemaName() {
        return this.name;
    }

    @Override
    public String toString() {
        StringBuilder returnString = new StringBuilder();
        returnString.append(this.getSchemaName()).append(":\n---------------------------------------\n");

        for (Table value : this.tableMap.values()) {
            returnString.append(value.toString());
        }
        return returnString.toString();
    }

    private SqlTypeName stringToSqlTypeName (String SqlTypeString) {
        SqlTypeString = SqlTypeString.toLowerCase();
        switch (SqlTypeString) {
            case "integer":
                return SqlTypeName.INTEGER;
            case "date":
                return SqlTypeName.DATE;
            case "decimal":
                return SqlTypeName.DECIMAL;
            default:
                if (SqlTypeString.startsWith("char")) {
                    return SqlTypeName.CHAR;
                } else if (SqlTypeString.startsWith("varchar")) {
                    return SqlTypeName.VARCHAR;
                } else {
                    return null;
                }
        }
    }
}