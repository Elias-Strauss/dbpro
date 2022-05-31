package polydb.global;

import polydb.metadata.MetadataHandler;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class GlobalSchema extends AbstractSchema {

    final String database;
    final String system;


    GlobalSchema(String system, String database) {
        super();
        try {

            this.database = database;
            this.system = system;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Map<String, Table> getTableMap() {

        HashMap<String, Table> tableMap = new HashMap<>();

        try {

            JSONObject globalDBObj = MetadataHandler.getDBInfo(this.system);

            JSONArray globalTableArr = (JSONArray) globalDBObj.get("TABLES");

            //iterate through tables
            for (int i = 0; i < globalTableArr.length(); i++) {
                String table = globalTableArr.getJSONObject(i).get("TABLE_NAME").toString();
                String db = globalTableArr.getJSONObject(i).get("DATABASE_NAME").toString();
                String system = globalTableArr.getJSONObject(i).get("SYSTEM").toString();
                JSONArray globalTableAttrArr = (JSONArray) globalTableArr.getJSONObject(i).get("ATTRIBUTES");
                Tuple3<Integer, String, String>[] cols = new Tuple3[globalTableAttrArr.length()];
                //iterate through attributes
                for (int j = 0; j < globalTableAttrArr.length(); j++) {
                    String col = globalTableAttrArr.getJSONObject(j).get("ATTRIBUTE_NAME").toString();
                    String type = globalTableAttrArr.getJSONObject(j).get("ATTRIBUTE_TYPE").toString();
                    Integer order = (Integer) globalTableAttrArr.getJSONObject(j).get("ATTRIBUTE_ORDER");
                    Tuple3 t3 = new Tuple3(col, type, order);

                    cols[order] = t3;

                }

                tableMap.put(table, new GlobalTable(cols, table, db, system));

            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        return tableMap;
    }
}
