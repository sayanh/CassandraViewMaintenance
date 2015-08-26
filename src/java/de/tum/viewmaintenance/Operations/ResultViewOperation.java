package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 8/16/15.
 */
public class ResultViewOperation extends GenericOperation {

    private static final Logger logger = LoggerFactory.getLogger(ResultViewOperation.class);
    final static String WHERE_TABLE_INDENTIFIER = "_where_";
    final static String JOIN_TABLE_INDENTIFIER = "_innerjoin_";
    final static String AGG_TABLE_INDENTIFIER = "_agg";
    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering insert trigger for Final Result Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure {}", this.inputViewTables);
        logger.debug("##### Trigger request :: " + triggerRequest);

        try {
            if ( inputViewTables.size() >= 1 && inputViewTables.get(0).getName().contains(WHERE_TABLE_INDENTIFIER) ) {
                whereInsertTrigger(triggerRequest);
            } else if ( inputViewTables.size() == 1 && inputViewTables.get(0).getName().contains(JOIN_TABLE_INDENTIFIER) ) {

            } else if ( inputViewTables.size() == 1 && inputViewTables.get(0).getName().contains(AGG_TABLE_INDENTIFIER) ) {

            }
        } catch ( Exception e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
            throw e;
        }

        return true;
    }


    private void whereInsertTrigger(TriggerRequest triggerRequest) {

        Table resultTable = operationViewTables.get(0);
        Map<String, ColumnDefinition> resultTableDesc = ViewMaintenanceUtilities.getTableDefinitition(resultTable.getKeySpace(),
                resultTable.getName());
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        PrimaryKey whereTablePrimaryKey = null;
        Table whereTableInvolved = null;

        Map<String, ColumnDefinition> whereViewTableDesc = null;
        for ( Table whereTable : inputViewTables ) {
            String targetTableDerivedFromOperationTable = whereTable.getName().split("_")[2];
            if ( targetTableDerivedFromOperationTable.equalsIgnoreCase(triggerRequest.getBaseTableName()) ) {
                whereTableInvolved = whereTable;
                whereViewTableDesc = ViewMaintenanceUtilities.getTableDefinitition(whereTable.getKeySpace(),
                        whereTable.getName());
                break;
            }
        }

        Map<String, List<String>> userData = new HashMap<>(); // Source is DataJson and whereTable

        for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : whereViewTableDesc.entrySet() ) {
            Iterator dataIter = keySet.iterator();
            while ( dataIter.hasNext() ) {
                String tempDataKey = (String) dataIter.next();
                logger.debug("### Checking -- Key: " + tempDataKey);
                logger.debug("### Checking -- Value: " + dataJson.get(tempDataKey));
                if ( columnDefinitionEntry.getKey().equalsIgnoreCase(tempDataKey) ) {
                    List<String> tempColValue = new ArrayList<>();
                    tempColValue.add(columnDefinitionEntry.getValue().type.toString());
                    if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue()
                            .type.toString()).equalsIgnoreCase("Integer") ) {
                        tempColValue.add((String) dataJson.get(tempDataKey));
                    } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue()
                            .type.toString()).equalsIgnoreCase("String") ) {
                        tempColValue.add(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                    }
                    if ( columnDefinitionEntry.getValue().isPartitionKey() ) {
                        tempColValue.add("true");
                        whereTablePrimaryKey = new PrimaryKey(columnDefinitionEntry.getKey(), columnDefinitionEntry.getValue().type.toString(),
                                ((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        logger.debug("### whereTablePrimaryKey :: " + whereTablePrimaryKey);
                    } else {
                        tempColValue.add("false");
                    }
                    userData.put(columnDefinitionEntry.getKey(), tempColValue);
                    break;
                }
            }
        }
        logger.debug("### User data created out of the where view table and dataJson :: " + userData);

        // Query the where table if this data is there or not
        Row existingRecordWhereTable = ViewMaintenanceUtilities.getExistingRecordIfExists(whereTablePrimaryKey, whereTableInvolved);


        // Map for result view table along the lines of userData
        Map<String, List<String>> userDataForResultViewTable = new HashMap<>();
        PrimaryKey resultTablePrimaryKey = null;

        // Mapping where view table vs result view table


        for ( Map.Entry<String, List<String>> userDataCol : userData.entrySet() ) {
            for ( Map.Entry<String, ColumnDefinition> resultViewCol : resultTableDesc.entrySet() ) {
                if ( userDataCol.getKey().equalsIgnoreCase(resultViewCol.getKey()) ||
                        resultViewCol.getKey().equalsIgnoreCase(userDataCol.getKey() + "_temp") ) {
                    List<String> tempColValResultView = new ArrayList<>();
                    tempColValResultView.add(resultViewCol.getValue().type.toString());
                    tempColValResultView.add(userDataCol.getValue().get(1));
                    tempColValResultView.add(userDataCol.getValue().get(2));
                    userDataForResultViewTable.put(resultViewCol.getKey(), tempColValResultView);
                    if ( userDataCol.getValue().get(2).equalsIgnoreCase("true") ) {
                        resultTablePrimaryKey = new PrimaryKey(resultViewCol.getKey(), resultViewCol.getValue().type.toString(),
                                userDataCol.getValue().get(1));
                    }
                    break;
                }
            }
        }

        logger.debug("### Result View table record created :: " + userDataForResultViewTable);
        logger.debug("### resultTablePrimaryKey :: " + resultTablePrimaryKey);

        if ( existingRecordWhereTable != null ) {
            logger.debug("#### Existing record in the whereViewTable #### " + existingRecordWhereTable);

            Row existingRecordResultView = ViewMaintenanceUtilities.getExistingRecordIfExists(resultTablePrimaryKey,
                    resultTable);

            if ( existingRecordResultView != null ) {
                // Get existing record from the result view and update
                logger.debug("### Record already exists... Update of the old record is in progress!! " + existingRecordResultView );
                updateResultView(resultTablePrimaryKey, userDataForResultViewTable);
            } else {
                // Fresh new insert
                logger.debug("### New entry for the record");
                insertIntoResultView(userDataForResultViewTable);
            }


        } else {

            logger.debug("#### Record does not exist in the whereViewTable ####" );
            // Either it is not there or if it is there then it needs to be deleted from result view
            Row existingRecordResultView = ViewMaintenanceUtilities.getExistingRecordIfExists(resultTablePrimaryKey,
                    resultTable);

            if ( existingRecordResultView != null ) {
                logger.debug("#### Existing record found in the result view table :: " + existingRecordResultView);
                deleteFromResultView(resultTablePrimaryKey);
            } else {
                // Nothing needs to be done
                logger.debug("### Record was not found in the result view table and need not to be dealt with!!!");
                return;
            }
        }


    }


    private void deleteFromResultView(PrimaryKey resultTablePrimaryKey) {
        Statement deleteQuery = null;

        if ( resultTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

            deleteQuery = QueryBuilder.delete().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(resultTablePrimaryKey.getColumnName(),
                    Integer.parseInt(resultTablePrimaryKey.getColumnValueInString())));
        } else if ( resultTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {

            deleteQuery = QueryBuilder.delete().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(resultTablePrimaryKey.getColumnName(),
                    resultTablePrimaryKey.getColumnValueInString()));
        }

        logger.debug("#### Delete query from result view table :: " + deleteQuery);
        CassandraClientUtilities.commandExecution("localhost", deleteQuery);

    }


    private void updateResultView(PrimaryKey resultTablePrimaryKey, Map<String, List<String>> userDataForResult) {

        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Statement updateQuery = null;
        for ( Map.Entry<String, List<String>> userDataCol : userDataForResult.entrySet() ) {
            if ( userDataCol.getValue().get(2).equalsIgnoreCase("true") ) {
                // For primary key
                if ( resultTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    updateQuery = assignments.where(QueryBuilder.eq(resultTablePrimaryKey.getColumnName(),
                            Integer.parseInt(resultTablePrimaryKey.getColumnValueInString())));
                } else if ( resultTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                    updateQuery = assignments.where(QueryBuilder.eq(resultTablePrimaryKey.getColumnName(),
                            resultTablePrimaryKey.getColumnValueInString()));
                }
            } else {
                // For non-primary key
                if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userDataCol.getValue().get(0))
                        .equalsIgnoreCase("Integer") ) {
                    assignments.and(QueryBuilder.add(userDataCol.getKey(), Integer.parseInt(userDataCol.getValue().get(1))));
                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userDataCol.getValue().get(0))
                        .equalsIgnoreCase("String") ) {
                    assignments.and(QueryBuilder.add(userDataCol.getKey(), userDataCol.getValue().get(1)));
                }

            }


            logger.debug("### Final update query for result view operation maintenance :: " + updateQuery);
            CassandraClientUtilities.commandExecution("localhost", updateQuery);
        }
    }

    private void insertIntoResultView(Map<String, List<String>> userDataForResult) {
        List<String> colNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();

        for ( Map.Entry<String, List<String>> userDataCol : userDataForResult.entrySet() ) {
            colNames.add(userDataCol.getKey());
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userDataCol.getValue().get(0))
                    .equalsIgnoreCase("Integer") ) {
                objects.add(Integer.parseInt(userDataCol.getValue().get(1)));
            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userDataCol.getValue().get(0))
                    .equalsIgnoreCase("String") ) {
                objects.add(userDataCol.getValue().get(1));
            }
        }

        Statement insertQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).values(colNames.toArray(new String[colNames.size()]),
                objects.toArray());

        logger.debug("#### Final insert query :: " + insertQuery);

        CassandraClientUtilities.commandExecution("localhost", insertQuery);
    }

    @Override
    public boolean updateTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public boolean deleteTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    public static ResultViewOperation getInstance(Row deltaTableRecord, List<Table> inputViewTables,
                                                  List<Table> operationViewTables) {
        ResultViewOperation resultViewOperation = new ResultViewOperation();
        resultViewOperation.setInputViewTables(inputViewTables);
        resultViewOperation.setOperationViewTables(operationViewTables);
        resultViewOperation.setDeltaTableRecord(deltaTableRecord);
        return resultViewOperation;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }


    public void setInputViewTables(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public void setOperationViewTables(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    @Override
    public String toString() {
        return "ResultViewOperation{" +
                "\n deltaTableRecord=" + deltaTableRecord +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }
}
