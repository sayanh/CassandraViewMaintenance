package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.trigger.*;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by shazra on 6/19/15.
 * <p/>
 * This class is meant to read the commitlogsv2 and take actions to maintain the views based on the events.
 */
public class ViewMaintenanceLogsReader extends Thread {

    private static volatile ViewMaintenanceLogsReader instance;
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "viewMaintenceCommitLogsv2.log";
    private static final String LOG_FILE_LOCATION = System.getProperty("cassandra.home") + "/logs/";
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceLogsReader.class);
    private static final int SLEEP_INTERVAL = 10000;


    public ViewMaintenanceLogsReader() {
        logger.debug("************************ Starting view maintenance infrastructure set up ******************");
        ViewMaintenanceConfig.readViewConfigFromFile();
        ViewMaintenanceConfig.setupViewMaintenanceInfrastructure();
        this.start();

    }

    private static void updateStatusFile(String lastOpertationIdProcessed) throws IOException {
        logger.debug("Inside updateStatusFile");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getProperty("cassandra.home") + "/data/" + STATUS_FILE));
        bufferedWriter.write(lastOpertationIdProcessed + "");
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    @Override
    public void run() {
        int lastOpertationIdProcessed;
        BufferedReader bufferedReader = null;
        Map<String, TriggerProcess> viewCache = new HashMap<>();
        while (true) {
            try {
                File statusFile = new File(System.getProperty("cassandra.home") + "/data/" + STATUS_FILE);
                if (statusFile.exists()) {
                    lastOpertationIdProcessed = Integer.parseInt(new String(Files.readAllBytes(Paths.get(System.getProperty("cassandra.home") + "/data/" + STATUS_FILE))));
                } else {
                    lastOpertationIdProcessed = 0;
                }

                logger.debug("Running the View Maintenance Engine !!!");
                bufferedReader = new BufferedReader(new FileReader(LOG_FILE_LOCATION + LOG_FILE));
                String tempLine = "";
                List<String> linesCommitLog = new ArrayList<>();
                while ((tempLine = bufferedReader.readLine()) != null) {
                    linesCommitLog.add(tempLine);
                }

                //TODO: use this List<String> stringList = Files.readAllLines(Paths.get(filePath), Charset.defaultCharset());
                try {
                    if (bufferedReader != null) {
                        bufferedReader.close();
                    }
                } catch (IOException e) {
                    logger.error("Error !! Stacktrace: \n " + ViewMaintenanceUtilities.getStackTrace(e));
                }
//                logger.debug("printing the list of tasks " + linesCommitLog);

                for (String lineActivity : linesCommitLog) {
                    JSONObject json;
                    Map<String, Object> retMap = new Gson().fromJson(lineActivity, new TypeToken<HashMap<String, Object>>() {
                    }.getType());
                    LinkedTreeMap dataJson = null;
                    String whereString = null;
                    String type = "";
                    int operation_id = 0;
                    String tableName = "";
                    TriggerRequest request = new TriggerRequest();


                    for (Map.Entry<String, Object> entry : retMap.entrySet()) {
                        String key = entry.getKey();
                        String value = "";
//                            logger.debug(key + "/" + entry.getValue());
                        if (key.equalsIgnoreCase("type")) {
                            type = (String) entry.getValue();
                            request.setType(type);
                        } else if (key.equalsIgnoreCase("operation_id")) {
                            operation_id = ((Double) entry.getValue()).intValue();
                        } else if (key.equalsIgnoreCase("data")) {
                            dataJson = (LinkedTreeMap) entry.getValue();
                            request.setDataJson(dataJson);
                        } else if (key.equalsIgnoreCase("table")) {
                            tableName = (String) entry.getValue();
                            if (tableName.contains(".")) {
                                String tableNameArr[] = tableName.split("\\.");
                                if (tableNameArr.length == 2) {
                                    request.setBaseTableKeySpace(tableNameArr[0]);
                                    request.setBaseTableName(tableNameArr[1]);
                                }
                            } else {
                                request.setBaseTableName(tableName);
                            }

                        } else if (key.equalsIgnoreCase("where")) {
//                                logger.debug("************** getting class ***************" + entry.getValue().getClass());
                            if (entry.getValue() instanceof String) {
                                whereString = (String) entry.getValue();
                                request.setWhereString(whereString);
                            }
                        }
                    }
                    if (lastOpertationIdProcessed < operation_id) {
                        // Action section
                        // Updating the delta view

                        DeltaViewTrigger deltaViewTrigger = new DeltaViewTrigger();
                        TriggerResponse deltaViewTriggerResponse = null;
                        if (type.equalsIgnoreCase("insert")) {
                            deltaViewTriggerResponse = deltaViewTrigger.insertTrigger(request);
                        } else if (type.equalsIgnoreCase("update")) {
                            deltaViewTriggerResponse = deltaViewTrigger.updateTrigger(request);
                        } else if (type.equalsIgnoreCase("delete")) {
                            deltaViewTriggerResponse = deltaViewTrigger.deleteTrigger(request);
                        }

                        if (!deltaViewTriggerResponse.isSuccess()) {
                            break;
                        }

                        Views views = Views.getInstance();
                        List<Table> viewsTables = views.getTables();
                        TriggerProcess triggerProcess = null;
                        TriggerResponse triggerResponse = null;
                        request.setViewKeyspace(views.getKeyspace());

                        // Printing Views
                        logger.debug("************** Views **************");
                        logger.debug(" Is the view retrieved = " + views);
                        logger.debug(" View properties = keyspace:{} with tableslist size:{} ", views.getKeyspace(), views.getTables().size());


                        //TODO : Read from the configuration which base table should be used to fill the views

                        // Usecase: Reverse Join View: "schematest.salary" is the second table for join.
                        if (tableName.contains("salary")) {
                            for (int i = 0; i < viewsTables.size(); i++) {
                                if (viewsTables.get(i).getName().equalsIgnoreCase("vt5")) {
                                    request.setViewTable(viewsTables.get(i));
                                    triggerProcess = new ReverseJoinViewTrigger();
                                    // Updating the reverse join view
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                }
                            }
                            if (triggerResponse.isSuccess()) {
                                // If result is successful
                                // Update the lastOperationProcessed variable
                                updateStatusFile(operation_id + "");
                            }
                        }
                        if (tableName.contains("emp")) {
                            for (int i = 0; i < viewsTables.size(); i++) {
//                                logger.debug("### Checking ### viewtable name ### " + viewsTables.get(i).getName());
                                if (viewsTables.get(i).getName().equalsIgnoreCase("vt1")) {
                                    triggerProcess = new SelectTrigger();
                                    request.setViewTable(viewsTables.get(i));
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
                                    } else if ("update".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (viewsTables.get(i).getName().equalsIgnoreCase("vt2")) {
                                    request.setViewTable(viewsTables.get(i));
                                    triggerProcess = new CountTrigger();
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (viewsTables.get(i).getName().equalsIgnoreCase("vt3")) {
                                    request.setViewTable(viewsTables.get(i));
                                    triggerProcess = new SumTrigger();
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }

                                } else if (viewsTables.get(i).getName().equalsIgnoreCase("vt4")) {
                                    request.setViewTable(viewsTables.get(i));
                                    triggerProcess = new PreAggregationTrigger();
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
                                    } else if ("update".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }

                                } else if (viewsTables.get(i).getName().equalsIgnoreCase("vt5")) {
                                    request.setViewTable(viewsTables.get(i));
                                    triggerProcess = new ReverseJoinViewTrigger();
                                    // Updating the reverse join view
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (viewsTables.get(i).getName().equalsIgnoreCase("vt6")) {
                                    request.setViewTable(viewsTables.get(i));
                                    triggerProcess = new SumCountTrigger();
                                    // Updating the reverse join view
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if ( viewsTables.get(i).getName().equalsIgnoreCase("vt7") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt8") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt9") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt10") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt11") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt12") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt13") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt14") ||
                                        viewsTables.get(i).getName().equalsIgnoreCase("vt15")) {
                                    if (viewCache.containsKey(viewsTables.get(i).getName())) {
                                        triggerProcess = viewCache.get(viewsTables.get(i).getName());
                                    } else {
                                        triggerProcess = new SQLViewMaintenanceTrigger();
                                    }
                                    Row deltaViewRow = null;
                                    if ("delete".equalsIgnoreCase(type)) {
                                        deltaViewRow = deltaViewTriggerResponse.getDeletedRowFromDeltaView();
                                    } else {
                                        deltaViewRow = deltaViewTriggerResponse.getDeltaViewUpdatedRow();
                                    }
                                    if (viewsTables.get(i).getSqlString() != null || viewsTables.get(i).getSqlString().equalsIgnoreCase("")) {
                                        triggerResponse = ((SQLViewMaintenanceTrigger)triggerProcess)
                                                .processSQLViewMaintenance(type, viewsTables.get(i),
                                                        deltaViewRow, request);
                                        if (!viewCache.containsValue(viewsTables.get(i).getName())){
                                            viewCache.put(viewsTables.get(i).getName(), triggerProcess);
                                        }
                                    }
                                }
                                if (!triggerResponse.isSuccess()) {
                                    break;
                                }
                            }

                            if (triggerResponse.isSuccess()) {
//                                    // If result is successful update the lastOperationProcessed variable and the status file
                                updateStatusFile(operation_id + "");
                            }
                        }
                    } else {
                        logger.debug("The view maintenance system has already processed id=" + operation_id);
                    }

                }
            } catch (Exception e) {
                logger.error("Error !! Stacktrace: \n " + ViewMaintenanceUtilities.getStackTrace(e));
            } finally {
                try {
                    logger.debug("Going to sleep now");
                    this.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                    logger.error("Error !! Stacktrace: \n " + ViewMaintenanceUtilities.getStackTrace(e));
                }
            }
//        */
        }
    }

    public static ViewMaintenanceLogsReader getInstance() {
        if (instance == null) {
            synchronized (ViewMaintenanceLogsReader.class) {
                if (instance == null) {
                    instance = new ViewMaintenanceLogsReader();
                }
            }
        }
        return instance;
    }

}