package polydb;

import polydb.core.queryinterface.SqlInterface;

public class testing {
    String calciteModel = "src/main/resources/inputconfig.json";
    String query = "INSERT INTO CSV.\"sink\" SELECT \"photoId\",CLEAN_STR(\"details\") FROM CSV.\"photos\"";
    public static void main(String[] args){
        System.out.println("asd");
        SqlInterface sl = new SqlInterface("src/main/resources/inputconfig.json");
        sl.getLogicalPlan("src/main/resources/inputconfig.json");
        System.out.println("asd");
    }
}
