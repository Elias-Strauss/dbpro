# dbpro

Basic functionality can be tested with ConsoleProgram.
Possible commands:
* sql-SQL query
  * Optimises a Query on the TPC-H schema through Calcite and prints the optimised plan
* run tpc-h
  * Optimises all TPC-H queries with Calcite and prints the optimised plans
* calcite
  * Runs TPC-H query1 with the Calcite intermediary to Spark
* Orca
  * Runs TPC-H query1 with the Orca intermediary to Spark
* exit
  * exits the program


DXL-parser: &nbsp;&nbsp;&nbsp; src/main/java/optimizers/orca/dxl-parser.py \
DXL-file: &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/main/resources/Orca_MiniDumps/q1.mdp \
Calcite-parser: src/main/java/test/RelAlgToSpark.java \
Spark-parser: &nbsp;&nbsp;src/main/java/execution_engines/JSONtpSpark_v2.java