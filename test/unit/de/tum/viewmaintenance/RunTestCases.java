package de.tum.viewmaintenance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 9/8/15.
 */
public class RunTestCases {
    private static final Logger logger = LoggerFactory.getLogger(RunTestCases.class);

    public static void main(String[] args) {
        RunTestCases runTestCases = new RunTestCases();

        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        runTestCases.resetBaseAndDeltaTables(load);
        ViewMaintenanceUtilities.deleteAllViews();

        try {
            Map<String, List<String>> testCasesMap = runTestCases.readTestCasesFile();
            for ( Map.Entry<String, List<String>> testCase : testCasesMap.entrySet() ) {

                logger.info("Analysis| #### Test Case::  " + testCase.getKey());
                System.out.println("#### Test Case::  " + testCase.getKey());

                logger.info("#### Executing the queries####### ");
                for ( String query : testCase.getValue() ) {
                    CassandraClientUtilities.commandExecution("localhost", query);
                }

                logger.debug("#### View maintenance process is on!!!!");

                Thread.sleep(10000);


                List<String> views = ViewMaintenanceUtilities.getAllViews();

                for ( String view : views ) {
                    logger.info("Analysis| ##### Printing the contents of the view {} ", view);
                    String viewNameArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(view);
                    Statement selectAllQuery = QueryBuilder.select().all().from(viewNameArr[0], viewNameArr[1]);
                    List<Row> existingRecords = CassandraClientUtilities.commandExecution("localhost", selectAllQuery);
                    logger.info("Analysis| Existing records = " + existingRecords);
                }

                logger.info("***************** Reset Cassandra DB Start *******************");

                runTestCases.resetBaseAndDeltaTables(load);
                ViewMaintenanceUtilities.resetAllViews();

                logger.info("***************** Reset Cassandra DB Complete *******************");


            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        System.exit(0);
    }


    private void resetBaseAndDeltaTables(Load load) {
        for ( Table table : load.getTables() ) {
            logger.debug("Table Name = " + table.getName());
            logger.debug("schema name = " + table.getKeySpace());
            logger.debug("Deleting the table....");
            Cluster cluster = CassandraClientUtilities.getConnection("localhost");
            CassandraClientUtilities.deleteTable(cluster, table.getKeySpace(), table.getName());
            CassandraClientUtilities.deleteTable(cluster, table.getKeySpace(), table.getName() + DeltaViewTrigger.DELTAVIEW_SUFFIX);
            logger.debug("Rebuilding the tables from the config...." + table);
            CassandraClientUtilities.createTable(cluster, table);
            Table deltaTable = CassandraClientUtilities.createDeltaViewTable(table);
            CassandraClientUtilities.createTable(cluster, deltaTable);
            CassandraClientUtilities.closeConnection(cluster);
            logger.debug("Table: " + table.getKeySpace() + "." + table.getName() + " creation complete!!!");
        }
    }


    private Map<String, List<String>> readTestCasesFile() {
        Map<String, List<String>> testCasesMap = new HashMap<>();
        try {
            String testCasesJson = new String(Files.readAllBytes(Paths.get(System.getProperty("cassandra.home")
                    + "/testcases.json")));
            Map<String, Object> retMap = new Gson().fromJson(testCasesJson, new TypeToken<HashMap<String, Object>>() {
            }.getType());

            for ( Map.Entry<String, Object> testCase : retMap.entrySet() ) {
                String testCategory = testCase.getKey();
                List<String> queries = (List<String>) ((LinkedTreeMap) testCase.getValue()).get("queries");
                logger.debug("#### Test category :: " + testCategory);

                logger.debug("#### Queries :: " + queries);

                testCasesMap.put(testCategory, queries);
            }
        } catch ( IOException e ) {
            logger.error(ViewMaintenanceUtilities.getStackTrace(e));
        }

        return testCasesMap;
    }
}
