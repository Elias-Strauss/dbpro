package polydb.mover;

import polydb.metadata.MetadataHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * HiveHBaseMover registers an HBase table on Hive.
 */
public class HiveHBaseMover {


    public static void move(String hiveJDBCUrl, String fromHBaseDB, String fromHBaseTable, String toHiveDB, String toHiveTable) throws IOException, SQLException {


        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();

        }
        Connection hive = DriverManager.getConnection(hiveJDBCUrl);


        JSONObject hbaseDBObj = MetadataHandler.getDBInfo("hbase");

        JSONArray hbaseTableArr = (JSONArray) hbaseDBObj.get("TABLES");

        JSONArray hbaseAttrsArr = null;
        //iterate through tables
        for (int i = 0; i < hbaseTableArr.length(); i++) {
            String table = hbaseTableArr.getJSONObject(i).get("TABLE_NAME").toString();
            if (table.equals(fromHBaseTable)) {
                JSONObject hbaseTableObj = hbaseTableArr.getJSONObject(i);
                hbaseAttrsArr = (JSONArray) hbaseTableObj.get("ATTRIBUTES");
            }

        }


        //iterate through columns
        String hbaseCols = "";
        String hiveCols = "(";

        //iterate through attributes
        for (int j = 0; j < hbaseAttrsArr.length(); j++) {

            String col = hbaseAttrsArr.getJSONObject(j).get("ATTRIBUTE_NAME").toString();

            String hiveCol = col;
            if (col.equals(":key"))
                hiveCol = "rowkey";
            hbaseCols += col + ", ";
            hiveCols += hiveCol.replace("cf:", "") + " STRING, ";

        }
        hbaseCols = hbaseCols.replaceAll(", $", "");
        hiveCols = hiveCols.replaceAll(", $", ")");

        Statement stmt = hive.createStatement();
        String delete = "DROP TABLE " + toHiveTable;
        stmt.execute(delete);


        String create = "CREATE EXTERNAL TABLE IF NOT EXISTS " + toHiveTable + "" + hiveCols + " \n" +
                "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n" +
                "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \"" + hbaseCols + "\") \n" +
                "TBLPROPERTIES(\"hbase.table.name\"=\"" + toHiveTable + "\")";

        stmt.execute("set hbase.zookeeper.quorum=zoo");
        stmt.execute(create);
        hive.close();

    }
}
