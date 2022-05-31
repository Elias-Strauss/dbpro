package polydb.metadata;

import polydb.mover.HiveHBaseMover;
import org.json.JSONObject;

import java.io.*;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The MetadataHandler is used to get the metadata from the available systems, and to initialize configuration parameters
 */
public class MetadataHandlerFile {

    public Properties configProperties;

    public MetadataHandlerFile() throws IOException {
        this.configProperties = new Properties();
        String propFileName = "config.properties";
        InputStream inputStream = MetadataHandlerFile.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            this.configProperties.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

    }

    public String getConnection(String system) {
        return this.configProperties.getProperty(system);

    }

    public static JSONObject getDBInfo(String system) throws IOException {


        JSONObject obj = new JSONObject();
        String propFileName = "dbinfo/" + system + ".json";
        InputStream inputStream = MetadataHandlerFile.class.getClassLoader().getResourceAsStream(propFileName);


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