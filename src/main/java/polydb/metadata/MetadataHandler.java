package polydb.metadata;

import polydb.mover.HiveHBaseMover;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The MetadataHandler is used to get the metadata from the available systems, and to initialize configuration parameters
 */
public class MetadataHandler {

    public Properties configProperties;

    public MetadataHandler() throws IOException {
        this.configProperties = new Properties();
        String propFileName = "config.properties";
        InputStream inputStream = MetadataHandler.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            this.configProperties.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

    }

    public static JSONObject get(String request) throws IOException {
        //System.out.println("GET request: " + request);

        URL url = new URL("http://meta-repo:8181/retrieve" + request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int status = connection.getResponseCode();

        JSONObject obj = new JSONObject();
        if (status == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            if (content.length() > 1)
                obj = new JSONObject(content.toString());
            else {
                obj = null;
                System.out.println(url);
                System.out.println("GET DID NOT WORK, content is probably empty.");
                System.out.println("content.toString() = " + content.toString());
            }
        } else {
            obj = null;
            System.out.println(url);
            System.out.println("GET DID NOT WORK");
        }

        return obj;
    }

    public String getConnection(String system) {
        return this.configProperties.getProperty(system);
    }

    public static Long getTableRowCount(String system, String db, String table) throws IOException {
        JSONObject ans = get("/tables/" + system + "/" + db + "/" + table);
        if (ans != null && ans.has("rowCount"))
            return ans.getLong("rowCount");
        else
            return 0L;
    }

    public static Long getDistinctColCount(String system, String db, String table, String col) throws IOException {
        return Math.round(get("/columns/" + system + "/" + db + "/" + table + "/" + col).getDouble("distinctCount"));
    }

    public static JSONObject getDBInfo(String system) throws IOException {


        JSONObject obj = new JSONObject();
        String propFileName = "dbinfo/" + system + ".json";
        InputStream inputStream = MetadataHandler.class.getClassLoader().getResourceAsStream(propFileName);

        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        String inputLine;
        StringBuilder content = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        if (content.length() > 1)
            obj = new JSONObject(content.toString());
        else {
            System.out.println("GET DID NOT WORK, content is probably empty.");
            System.out.println("content.toString() = " + content.toString());
        }


        return obj;
    }

    public void move(String fromSystem, String fromDB, String fromTable, String toSystem, String toDB, String toTable) {
        //TODO: extend for further system combinations
        if (fromSystem.toLowerCase().equals("hbase") && toSystem.toLowerCase().equals("hive")) {
            try {
                HiveHBaseMover.move(this.getConnection("hive"), fromDB, fromTable, toDB, toTable);
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        }
    }

}