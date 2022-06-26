package main;

import optimizers.calcite.SqlQueryParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConsoleInput {

    public static void main(String[] args)
            throws IOException, SqlParseException {
        while (true) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));

            String input = reader.readLine();

            SqlQueryParser SqlQueryParser = new SqlQueryParser();

            if (input.startsWith("SQL ")) {
                //1. SQL Parser
                //2. Optimizer
                System.out.println(input.substring(4));
                System.out.println(SqlQueryParser.parseString(input.substring(4)));
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
                    default:
                        System.out.println("Unknown command: \"" + input + "\"");
                }
            }


        }
    }

}
