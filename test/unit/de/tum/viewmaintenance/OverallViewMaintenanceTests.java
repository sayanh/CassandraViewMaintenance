package de.tum.viewmaintenance;

import de.tum.viewmaintenance.config.ViewMaintenanceConfig;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

/**
 * Created by shazra on 9/5/15.
 */
public class OverallViewMaintenanceTests {
    private static final Logger logger = LoggerFactory.getLogger(OverallViewMaintenanceTests.class);

    public static void main(String[] args) {
        boolean areTestsSuccessful = false;
        logger.info("Hello welcome to tests!!!!");
        logger.debug("Testing views!!");
        logger.debug("Reading views from the view config");
        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        for ( Table baseTable : load.getTables() ) {

            logger.debug(" Base table name : " + baseTable);
        }
        logger.debug("Reading view config file !!!");
        ViewMaintenanceConfig.readViewConfigFromFile();
        Views views = Views.getInstance();
//        Views views = loadGenerationProcess.readViewConfig();
        logger.debug(" Base tables involved!!");


        try {
            // Insert test data into Cassandra
            loadGenerationProcess.insertIntoEmpWithTestData("localhost");

            // Update test data in Cassandra
            loadGenerationProcess.updateEmpWithTestData("localhost");


            for ( Table viewTable : views.getTables() ) {
                logger.debug(" view table = " + viewTable.getKeySpace() + "." + viewTable.getName());
                switch ( viewTable.getName() ) {

                    case "vt7":
                        logger.debug("#### Testing view vt7 ####");
                        break;
                    case "vt8":
                        logger.debug("#### Testing view vt8 ####");
                        break;
                    case "vt9":
                        logger.debug("#### Testing view vt9 ####");
                        break;
                    case "vt10":
                        logger.debug("#### Testing view vt10 ####");
                        break;
                    case "vt11":
                        logger.debug("#### Testing view vt11 ####");
                        break;
                    case "vt12":
                        logger.debug("#### Testing view vt12 ####");
                        break;
                    case "vt13":
                        logger.debug("#### Testing view vt13 ####");
                        View13Test view13Test = new View13Test();
                        view13Test.startTest(viewTable);
                        break;
                    case "vt14":
                        logger.debug("#### Testing view vt14 ####");
                        break;
                    case "vt15":
                        logger.debug("#### Testing view vt15 ####");
                        break;
                    case "vt16":
                        logger.debug("#### Testing view vt16 ####");
                        break;
                    case "vt17":
                        logger.debug("#### Testing view vt17 ####");
                        break;

                    default:
                        logger.debug("No testing case available for " + viewTable.getName());
                }
            }
            areTestsSuccessful = true;
        } catch ( InterruptedException e ) {
            logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
        } catch ( Exception nohostExp ) {
            nohostExp.printStackTrace();
            logger.error("Cassandra is not up!!!!");
            logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(nohostExp));
            logger.error("Test client exits!!!!");
            System.exit(0);
        }


        if ( areTestsSuccessful ) {

            logger.debug("###### All tests ran ....!! ######");
        } else {

            logger.debug("###### Tests failed!! ######");
        }

        System.exit(0);
    }


}
