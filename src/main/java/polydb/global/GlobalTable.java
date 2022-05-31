package polydb.global;

import polydb.metadata.MetadataHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import scala.Tuple3;

import java.util.ArrayList;

public class GlobalTable extends AbstractTable {

    String name;
    Tuple3[] cols;
    String db;
    String system;

    /**
     * Creates a GlobalTable.
     */
    GlobalTable(Tuple3[] cols, String name, String db, String system) {
        this.name = name;
        this.cols = cols;
        this.db = db;
        this.system = system;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();


        for (Tuple3 col : this.cols) {

            SqlTypeName type;
            switch (col._2().toString()) {
                case "INTEGER":
                    type = SqlTypeName.INTEGER;
                    break;
                default:
                    type = SqlTypeName.VARCHAR;
                    break;
            }

            RelDataType sqlType = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(type), true);

            fieldInfo.add(col._1().toString(), sqlType);

        }
        return fieldInfo.build();
    }


    //TODO: towards join order optimization using external metastore
    @Override
    public Statistic getStatistic() {
        //return new StatisticImpl(name);

        final ImmutableBitSet uniqueKey = ImmutableBitSet.builder().set(0).build(); // assume that every table has the first column as its unique key
        ArrayList<ImmutableBitSet> uniqueKeyList = new ArrayList<>();
        uniqueKeyList.add(uniqueKey);
        Statistic stat = Statistics.UNKNOWN;

        //comment out for disabling statistics
        try {
            String sys = system;

            if (system.equalsIgnoreCase("hive"))
                sys = "hbase";

            long rowCount = MetadataHandler.getTableRowCount(sys, db, name);

            if (rowCount != 0)
                stat = Statistics.of(rowCount, uniqueKeyList);

        } catch (Exception e) {
            e.printStackTrace();

        }

        //

        return stat;

    }
}
