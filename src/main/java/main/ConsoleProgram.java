package main;

import execution_engines.JSONtoSpark_v2;
import optimizers.calcite.CalciteOptimizer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.spark.api.java.JavaRDD;
import org.jline.reader.*;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ConsoleProgram {

    public static void main(String[] args)
            throws IOException, SqlParseException {

        JSONtoSpark_v2 jsonToSpark_v2 = new JSONtoSpark_v2();
        String schemaPath = "src/main/resources/TPC-HTestDaten/CalciteSchema.json";
        CalciteOptimizer calciteOptimizer = new CalciteOptimizer(schemaPath);

        while (true) {

            Terminal terminal = TerminalBuilder.builder()
                    .system(true)
                    .build();

            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .history(new DefaultHistory())
                    .build();

            String input = lineReader.readLine().toLowerCase();
            if (input.startsWith("sql-")) {
                //1. SQL Parser
                //2. Optimizer
                String sqlQuery = input.substring(4);

                RelNode optimizedPlan = calciteOptimizer.optimizeQuery(sqlQuery);

                System.out.println(optimizedPlan.explain());

            } else if (input.startsWith("run tpc-h")) {
                AtomicInteger queryNumber = new AtomicInteger(-1);
                if (isParsable(input.substring(9).trim())) {
                    queryNumber.set(Integer.parseInt(input.substring(9).trim()));
                }

                    try (Stream<Path> paths = Files.walk(Paths.get("src/main/resources/tpc-h_queries"))) {

                        AtomicInteger queryCount = new AtomicInteger(1);

                        paths.filter(Files::isRegularFile)
                            .forEach(queryPath -> {

                                if (queryCount.get() == queryNumber.get() || queryNumber.get() == -1) {

                                    //get file content
                                    System.out.println("TPC-H Query " + queryCount.get() + ":\n");

                                    try {
                                        String query = Files.readString(queryPath);
                                        //filter out comments starting with : or -
                                        String[] lines = query.split("\n");

                                        StringBuilder stringBuilder = new StringBuilder();
                                        for (String s:lines) {
                                            if (!(s.startsWith(":") || s.startsWith("-"))) {
                                                stringBuilder.append(s.replace('\r', ' '));
                                            }
                                        }

                                        String sqlQuery = stringBuilder.toString();


                                        System.out.println(calciteOptimizer.optimizeQuery(sqlQuery).explain());

                                    } catch (IOException e) {
                                        System.err.println(e.getMessage());
                                        throw new RuntimeException(e);
                                    } catch (SqlParseException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                                queryCount.getAndIncrement();
                            });
                    }


            }else {
                switch (input) {
                    case "calcite":
                        //Runs the TPC-H query1 json from Calcite on Spark
                        //Our Calcite output parser does not 100% conform to our Orca JSON, that is
                        //our referenz for Spark, as such
                        //small adjustments to the RelAlgToSpark.java are still required
                        Path filePath = Path.of("src/main/resources/calcite_TPC-H.json");
                        String jsonContent = Files.readString(filePath);
                        JavaRDD<ArrayList<Object>> javaRDDCalcite = jsonToSpark_v2.translate(jsonContent);

                        javaRDDCalcite.collect().forEach(System.out::println);
                        break;
                    case "exit":
                        return;
                    case "orca":
                        //Runs the python DXL parser on TPC-H query1 and executes Spark
                        Path pythonDXLParser = Paths.get("./src/main/java/optimizers/orca/dxl-parser.py");
                        Path miniDump = Paths.get("./src/main/resources/Orca_MiniDumps/q1.mdp");

                        String line = "python " + pythonDXLParser.toAbsolutePath() + " " + miniDump.toAbsolutePath();
                        System.out.println(line);
                        CommandLine cmdLine = CommandLine.parse(line);

                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

                        DefaultExecutor executor = new DefaultExecutor();
                        executor.setStreamHandler(streamHandler);

                        int exitCode = executor.execute(cmdLine);

                        //System.out.println(outputStream.toString(Charset.defaultCharset()));

                        JavaRDD<ArrayList<Object>> javaRDDOrca = jsonToSpark_v2.translate(outputStream.toString(Charset.defaultCharset()));

                        javaRDDOrca.collect().forEach(System.out::println);

                        break;
                    default:
                        System.out.println("Unknown command: \"" + input + "\"");
                }
            }


        }
    }

    private static boolean isParsable(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (final NumberFormatException e) {
            return false;
        }
    }

}
