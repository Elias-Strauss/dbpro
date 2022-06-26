package optimizers.calcite;

import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class CsvTable extends AbstractTable {

    private final String name;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;
    private final CsvTableStatistic statistic;
    private RelDataType rowType;

    public CsvTable(String name, List<String> fieldNames, List<SqlTypeName> fieldTypes, CsvTableStatistic statistic) {
        this.name = name;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.statistic = statistic;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());

            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = relDataTypeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }

            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
        }

        return rowType;
    }
}
