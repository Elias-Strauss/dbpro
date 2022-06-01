package polydb.rest.service;

import com.polydb.examples.udfs.StringCleaner;
import com.polydb.pipelines.SQLRheemPipeline;
import com.polydb.pipelines.ScalarUdf;
import com.polydb.rest.MyResponse;
import com.polydb.rest.Query;
import org.apache.calcite.rel.externalize.RelJsonWriter;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@Path("/query")
public class QueryService {
    private String message = null;

    @POST
    @Path("/submit")
    @Produces(MediaType.APPLICATION_JSON)
    public Response Plans(Query q)
            throws Exception {

        //if (!queryStr.equals("")) {
        SQLRheemPipeline pipeline = new SQLRheemPipeline("/inputconfig.json");
        pipeline.setCsvSinkPath("hdfs://namenode:9000/data/");
        pipeline.setJdbcOutputProperties("/jdbc_sink.properties");

        ScalarUdf clean_str = new ScalarUdf("CLEAN_STR", StringCleaner.class, new Class<?>[]{String.class});
        pipeline.registerUdf(clean_str);

        MyResponse myresp = new MyResponse();
        RelJsonWriter jsonWriter = new RelJsonWriter();
        String queryStr = q.getQuery();
        myresp.setQuery(queryStr);

        System.out.println("Optimizing query:");
        System.out.println(queryStr);
        pipeline.optimize(queryStr);

        //get & set validated plan

        pipeline.validatedPlan.explain(jsonWriter);
        String relJson = jsonWriter.asString();
        myresp.setValidatedPlan(relJson);

        RelJsonWriter jsonWriter2 = new RelJsonWriter();

        System.out.println("Unoptimized plan:");
        System.out.println(myresp.getValidatedPlan());
        //get & set optimized plan

        pipeline.optimizedPlan.explain(jsonWriter2);
        relJson = jsonWriter2.asString();
        myresp.setOptimizedPlan(relJson);

        System.out.println("Optimized plan:");
        System.out.println(myresp.getOptimizedPlan());

        //TODO: get correct wayang & execution plan
        myresp.setWayangPlan("");
        myresp.setPlatformPlan("");

        System.out.println("Wayang plan:");
        System.out.println(myresp.getWayangPlan());

        pipeline.execute();


        return Response.status(200).entity(myresp).build();


        //} else
        //  return Response.status(400).entity("Please specify a query").build();

    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)    // JSON
    public String Status()
            throws InternalServerErrorException {

        return "{\"status\":\"1\"}";

    }

    @GET
    @Path("/sample")
    @Produces(MediaType.TEXT_PLAIN)
    public String Sample()
            throws InternalServerErrorException, IOException {

        return readFromInputStream(QueryService.class.getClassLoader().getResourceAsStream("calcite_sample_plan.json"));
    }

    private String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }
}
