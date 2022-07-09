package main;

import optimizers.calcite.CalciteOptimizer;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.jline.reader.*;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ConsoleProgram {

    public static void main(String[] args)
            throws IOException, SqlParseException {
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
                String schemaPath = "src/main/resources/TPC-HTestDaten/CalciteSchema.json";


                CalciteOptimizer calciteOptimizer = new CalciteOptimizer(schemaPath);

                System.out.println(calciteOptimizer.optimizeQuery(sqlQuery).explain());

            } else {
                switch (input){
                    case "exit":
                        return;
                    case "python":
                        //Demo to run a python programm
                        //Path doesn't work :(
                        Path path = Paths.get("hello.py");

                        String line = "python C:\\Users\\Reste\\OneDrive\\Desktop\\dbpro\\src\\main\\java\\optimizers\\orca\\hello.py";
                        System.out.println(line);
                        CommandLine cmdLine = CommandLine.parse(line);

                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

                        DefaultExecutor executor = new DefaultExecutor();
                        executor.setStreamHandler(streamHandler);

                        int exitCode = executor.execute(cmdLine);
                        System.out.println(outputStream);
                        break;
                    case "run tpc-h":

                        try (Stream<Path> paths = Files.walk(Paths.get("src/main/resources/tpc-h_queries"))) {


                            String schemaPath = "src/main/resources/TPC-HTestDaten/CalciteSchema.json";
                            CalciteOptimizer calciteOptimizer = new CalciteOptimizer(schemaPath);

                            AtomicInteger queryCount = new AtomicInteger(1);

                            paths.filter(Files::isRegularFile)
                                    .forEach(queryPath -> {
                                        System.out.println("TPC-H Query " + queryCount + ":\n");
                                        queryCount.getAndIncrement();

                                        //get file content
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

                                    });
                        }

                        break;
                    default:
                        System.out.println("Unknown command: \"" + input + "\"");
                }
            }


        }
    }

}
