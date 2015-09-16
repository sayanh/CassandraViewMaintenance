package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import net.sf.jsqlparser.expression.Expression;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 8/14/15.
 */


class UtilityProcessor {
    List<String> userData = null; // For target column: FunctionName, Value, TargetColumnName
    List<String> aggregationKeyData = null; // For curr aggregation key: columnName, Cass Type
    PrimaryKey preAggTablePK = null;
    PrimaryKey baseTablePK = null;

    public void setUserData(List<String> userData) {
        this.userData = userData;
    }

    public void setAggregationKeyData(List<String> aggregationKeyData) {
        this.aggregationKeyData = aggregationKeyData;
    }

    public void setPreAggTablePK(PrimaryKey preAggTablePK) {
        this.preAggTablePK = preAggTablePK;
    }

    public void setBaseTablePK(PrimaryKey baseTablePK) {
        this.baseTablePK = baseTablePK;
    }

    @Override
    public String toString() {
        return "UtilityProcessor{" +
                "userData=" + userData +
                ", aggregationKeyData=" + aggregationKeyData +
                ", preAggTablePK=" + preAggTablePK +
                ", baseTablePK=" + baseTablePK +
                '}';
    }
}


public class PreAggOperation extends GenericOperation {

    private static final Logger logger = LoggerFactory.getLogger(PreAggOperation.class);
    private static final List<String> AVAILABLE_FUNCS = Arrays.asList("sum", "count", "min", "max");
    final static String WHERE_TABLE_INDENTIFIER = "_where_";
    final static String INNER_JOIN_TABLE_INDENTIFIER = "_innerjoin_";
    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Table viewConfig;
    private List<Table> whereTables;


//    public List<Table> getWhereTables() {
//        return whereTables;
//    }

    public void setWhereTables(List<Table> whereTables) {
        this.whereTables = whereTables;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
    }

    public void setInputViewTables(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public void setOperationViewTables(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

//    public void setDeltaTableRecord(Row deltaTableRecord) {
//        this.deltaTableRecord = deltaTableRecord;
//    }

    public static PreAggOperation getInstance(List<Table> inputViewTables,
                                              List<Table> operationViewTables) {
        PreAggOperation preAggOperation = new PreAggOperation();
        preAggOperation.setInputViewTables(inputViewTables);
        preAggOperation.setOperationViewTables(operationViewTables);
        return preAggOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering insert trigger for PreAggregate Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure {}", this.inputViewTables);
        logger.debug("##### Trigger request :: " + triggerRequest);

        if ( inputViewTables == null || inputViewTables.get(0).getName().contains(WHERE_TABLE_INDENTIFIER) ) {
            // Case: Either where or deltaview in the input for this view operation

            String baseTableInvolved = viewConfig.getRefBaseTable();
            String baseTableInvolvedArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(baseTableInvolved);

            Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities.getTableDefinitition(baseTableInvolvedArr[0],
                    baseTableInvolvedArr[1]);
            logger.debug("#### Base table description :: " + baseTableDesc);

            UtilityProcessor utilityProcessor = utilityProcessor(triggerRequest);


            List<String> userData = utilityProcessor.userData; // For target column: FunctionName, Value, TargetColumnName
            List<String> aggregationKeyData = utilityProcessor.aggregationKeyData; // For curr aggregation key: columnName, Cass Type
            PrimaryKey preAggTablePK = utilityProcessor.preAggTablePK;
            PrimaryKey baseTablePK = utilityProcessor.baseTablePK;

//            List<String> userData = new ArrayList<>(); // For target column: FunctionName, Value, TargetColumnName
//            List<String> aggregationKeyData = new ArrayList<>(); // For curr aggregation key: columnName, Cass Type
//            PrimaryKey preAggTablePK = null;
//            PrimaryKey baseTablePK = null;
//
//            for ( Column column : operationViewTables.get(0).getColumns() ) {
//                String derivedColumnName = column.getName().substring(column.getName().indexOf("_") + 1);
//                String prefixForColName = column.getName().substring(0, column.getName().indexOf("_"));
//                if ( AVAILABLE_FUNCS.contains(prefixForColName) ) {
//                    userData.add(prefixForColName);
//                    userData.add(((String) dataJson.get(derivedColumnName)).replaceAll("'", ""));
//                    userData.add(derivedColumnName);
//                } else {
//
//                    for ( Map.Entry<String, ColumnDefinition> columnDefBaseTableEntry : baseTableDesc.entrySet() ) {
//                        if ( derivedColumnName.equalsIgnoreCase(columnDefBaseTableEntry.getKey()) ) {
//
//                            aggregationKeyData.add(derivedColumnName);
//                            aggregationKeyData.add(columnDefBaseTableEntry.getValue().type.toString());
//                            preAggTablePK = new PrimaryKey(column.getName(), columnDefBaseTableEntry.getValue().type.toString(),
//                                    ((String) dataJson.get(derivedColumnName)).replaceAll("'", ""));
//                        }
//
//                        if ( columnDefBaseTableEntry.getValue().isPartitionKey() ) {
//                            baseTablePK = new PrimaryKey(columnDefBaseTableEntry.getKey(), columnDefBaseTableEntry
//                                    .getValue().type.toString(), ((String) dataJson.get(columnDefBaseTableEntry.getKey()))
//                                    .replaceAll("'", ""));
//                        }
//                    }
//                }
//            }

            logger.debug("### User data generated which corresponds to the target column :: " + userData);

            logger.debug("### BaseTable primary key :: " + baseTablePK);

            String statusEntryColAggKey = ViewMaintenanceUtilities.checkForChangeInAggregationKeyInDeltaView(
                    aggregationKeyData, deltaTableRecord);


            Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                    operationViewTables.get(0));


            if ( inputViewTables != null && inputViewTables.get(0).getName().contains(WHERE_TABLE_INDENTIFIER) ) {
                Table whereTable = inputViewTables.get(0);

                if ( whereTable.isMaterialized() ) { // It is materialized at the moment
                    Row existingRecordInWhereView = ViewMaintenanceUtilities.getExistingRecordIfExists(baseTablePK,
                            whereTable);

                    if ( existingRecordInWhereView == null ) { // Where table does not contain the data

                        if ( statusEntryColAggKey.equalsIgnoreCase("new") ) {
                            // The new entry was not satisfying in where clause
                            // Nothing needs to be and return
                            logger.debug("### Newly entered column does not satisfy the where conditions!!! ");
                        } else {
                            logger.debug("###  The old entry got deleted in the where due to update " +
                                    "which does not satisfy where clause now!!");

                            // Check whether the old value was existing or not.
                            Table concernedWhereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
                                    triggerRequest, inputViewTables);

                            logger.debug("#### Received concerned where table :: " + concernedWhereTable);

                            boolean didOldValueSatisfyWhereClause = ViewMaintenanceUtilities.didOldValueSatisfyWhereClause
                                    (viewConfig, triggerRequest, userData, deltaTableRecord, concernedWhereTable);
                            if ( didOldValueSatisfyWhereClause ) {
                                logger.debug(" Old agg key satisfied the where clause hence it needs to be deleted");
                                intelligentDeletionPreAggViewTable(userData, aggregationKeyData, baseTableInvolvedArr);
                            } else {
                                logger.debug("### Old value did not exist and so did the new value hence nothing needs to be done!");
                            }

                        }

                        logger.debug("### PreAggregation View Maintenance(entry: where view) is complete!!");

                    } else {

                        logger.debug("#### Checking .... statusEntryColAggKey = {}", statusEntryColAggKey);

                        if ( statusEntryColAggKey.equalsIgnoreCase("new") ) {
                            logger.debug("###Case: New row with where table entry");
                            intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);

                        } else if ( statusEntryColAggKey.equalsIgnoreCase("changed")
                                || statusEntryColAggKey.equalsIgnoreCase("unchanged") ) {
                            logger.debug("###Case: Un/Ch-anged agg key with where table entry");
                            // Check whether the old value was existing or not.
                            Table concernedWhereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
                                    triggerRequest, inputViewTables);

                            logger.debug("#### Received concerned where table :: " + concernedWhereTable);

                            boolean didOldValueSatisfyWhereClause = ViewMaintenanceUtilities.didOldValueSatisfyWhereClause
                                    (viewConfig, triggerRequest, userData,
                                            deltaTableRecord, concernedWhereTable);

                            if ( didOldValueSatisfyWhereClause ) {
                                logger.debug("#### Old value was present hence deleting that value!!!");
                                intelligentDeletionPreAggViewTable(userData, aggregationKeyData, baseTableInvolvedArr);

                                logger.debug("#### Update with new value !!!");
                                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
                            } else {
                                logger.debug("#### Update with new value !!!");
                                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
                            }

//                        } else if ( statusEntryColAggKey.equalsIgnoreCase("unchanged") ) {
//                            logger.debug("###Case: Unchanged agg key and changed agg target col with where table entry");
//
                        }


                    }
                    return true;

                } else {
                    //TODO: Process the where view in memory
                }


            }


            // check for the change in aggregation key over the time
            if ( statusEntryColAggKey.equalsIgnoreCase("changed") ) {
                // Aggregation key got changed

                // Insert or update intelligently based on the data.
                // This situation is true for where and non where cases
                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);

                // Delete(actually update) the amount for the old aggregation key
                // If where is the entry then check the old value satisfied where or not
                // If it did not satisfy then no need to delete

//                if ( inputViewTables != null && inputViewTables.get(0).getName().contains(WHERE_TABLE_INDENTIFIER) ) {
//
//                    logger.debug("### Where agg(may be entry may be not!!) value changed!!!");
//                    Table concernedWhereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
//                            triggerRequest, inputViewTables);
//
//                    logger.debug("#### Received concerned where table :: " + concernedWhereTable);
//
//                    boolean didOldValueSatisfyWhereClause = ViewMaintenanceUtilities.didOldValueSatisfyWhereClause
//                            (viewConfig, triggerRequest, userData,
//                                    deltaTableRecord, concernedWhereTable);
//
//                    logger.debug("### Did old value satisfy where clause ? " + didOldValueSatisfyWhereClause);
//                    logger.debug("### aggregation key data :: " + aggregationKeyData);
//
//                    if ( didOldValueSatisfyWhereClause ) {
//                        logger.debug("#### Deletion of the old data pertaining to old agg key!!! Where case");
//                        intelligentDeletionPreAggViewTable(userData, aggregationKeyData, baseTableInvolvedArr);
//                    }
//                } else {
                logger.debug("#### Deletion of the old data pertaining to old agg key!!! no- where case!!!");
                intelligentDeletionPreAggViewTable(userData, aggregationKeyData, baseTableInvolvedArr);
//                }


            } else {
                // Aggregation key remained same or new entry
                if ( statusEntryColAggKey.equalsIgnoreCase("new") ) {
                    intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
                } else if ( statusEntryColAggKey.equalsIgnoreCase("unchanged") ) {
                    String statusTargetCol = checkForChangeInTargetColValue(userData);

                    if ( statusTargetCol.equalsIgnoreCase("changed") ) {
                        // Target column has changed!!
                        if ( userData.get(0).equalsIgnoreCase("sum") ) {

//                            if ( whereTables != null ) {
//
//                                Table concernedWhereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
//                                        triggerRequest, inputViewTables);
//
//                                logger.debug("#### Received concerned where table :: " + concernedWhereTable);
//
//                                boolean didOldValueSatisfyWhereClause = ViewMaintenanceUtilities.didOldValueSatisfyWhereClause(viewConfig, triggerRequest,
//                                        userData, deltaTableRecord, concernedWhereTable);
//
//                                logger.debug("### Did old value satisfy where clause ? " + didOldValueSatisfyWhereClause);
//
//                                if ( didOldValueSatisfyWhereClause ) {
//                                    logger.debug("#### Old record was present in where previously !!");
//                                    logger.debug("#### Need to update the existing value!! ");
//                                    logger.debug("#### Checking --- preAggTablePK {} ", preAggTablePK);
//                                    logger.debug("#### Checking --- existingRecordPreAggTable {} ", existingRecordPreAggTable);
//                                    logger.debug("#### Checking --- user data : {}", userData);
//                                    updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
//                                } else {
//                                    logger.debug("#### Old value was not there in the where view previously!!! ");
//                                    logger.debug("#### Adding the sum value with the user data !!!  ");
//                                    updateSumPreAggViewOnlyAdding(preAggTablePK, existingRecordPreAggTable, userData);
//                                }
//
//                            } else {

                            updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
//                            }

                        } else if ( userData.get(0).equalsIgnoreCase("count") ) {
                            // Entry: Delta view - nothing needs to be done
                            // Entry: Where view or Join view with where view involvement - Newly included in where
                            // then update else nothing needs to be done.
                            if ( whereTables != null ) {
                                // If old target satisfied where? if not update else nothing needs to be done.

                                Table concernedWhereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
                                        triggerRequest, inputViewTables);

                                logger.debug("#### Received concerned where table :: " + concernedWhereTable);

                                boolean didOldValueSatisfyWhereClause = ViewMaintenanceUtilities.didOldValueSatisfyWhereClause(viewConfig, triggerRequest,
                                        userData, deltaTableRecord, concernedWhereTable);

                                logger.debug("### Did old value satisfy where clause ? " + didOldValueSatisfyWhereClause);

                                if ( didOldValueSatisfyWhereClause ) {
                                    logger.debug("#### No further computations required !!");
                                    logger.debug("#### It was there previously in the where view!! ");
                                } else {
                                    logger.debug("#### Old value was not there in the where view previously!!! ");
                                    logger.debug("#### Incrementing count value by one!!!  ");
                                    updateCountPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
                                }

                            }

                        } else if ( userData.get(0).equalsIgnoreCase("max") ) {
                            // TODO:: Yet to be implemented
                        } else if ( userData.get(0).equalsIgnoreCase("min") ) {
                            // TODO:: Yet to be implemented
                        }
                    }
                }
            }


        } else if ( inputViewTables == null || inputViewTables.get(0).getName().contains(INNER_JOIN_TABLE_INDENTIFIER) ) {

            processInnerJoinEntryForPreAggOperation(triggerRequest);

        }
        return true;

    }

    private void processInnerJoinEntryForPreAggOperation(TriggerRequest triggerRequest) {

        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Table innerJoinTableConfig = inputViewTables.get(0);
        Map<String, ColumnDefinition> innerJoinTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                innerJoinTableConfig.getKeySpace(), innerJoinTableConfig.getName());
        PrimaryKey innerJoinPrimaryKeyCur = ViewMaintenanceUtilities.getJoinTablePrimaryKey(innerJoinTableDesc,
                dataJson);

        List<String> joinKeyData = new ArrayList<>();
        joinKeyData.add(innerJoinPrimaryKeyCur.getColumnName());
        joinKeyData.add(innerJoinPrimaryKeyCur.getColumnInternalCassType());
        String statusCurJoinKey = ViewMaintenanceUtilities.checkForChangeInJoinKeyInDeltaView(joinKeyData, deltaTableRecord);

        UtilityProcessor utilityProcessor = utilityProcessor(triggerRequest);


        List<String> userData = utilityProcessor.userData; // For target column: FunctionName, Value, TargetColumnName
//        List<String> aggregationKeyData = utilityProcessor.aggregationKeyData; // For curr aggregation key: columnName, Cass Type
        PrimaryKey preAggTablePK = utilityProcessor.preAggTablePK;
//        PrimaryKey baseTablePK = utilityProcessor.baseTablePK;

        Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                operationViewTables.get(0));

        Row existingRecordInInnerJoin = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKeyCur,
                innerJoinTableConfig);

        if ( statusCurJoinKey.equalsIgnoreCase("new") ) {

            if ( existingRecordInInnerJoin != null ) {
                logger.debug("#### Case:: new ::: Update/Insert the aggregate key ");
                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
            }
        } else if ( statusCurJoinKey.equalsIgnoreCase("changed") ) {
            logger.debug("#### Case:: changed #####");
            PrimaryKey oldInnerJoinPrimaryKey = ViewMaintenanceUtilities.createOldJoinKeyfromNewValue(innerJoinPrimaryKeyCur,
                    deltaTableRecord);


            Row oldInnerJoinRecord = ViewMaintenanceUtilities.getExistingRecordIfExists(oldInnerJoinPrimaryKey,
                    innerJoinTableConfig);

            if (oldInnerJoinRecord == null) {

                logger.debug("#### Case:: changed :: The old join key does not exist in the inner join anymore!!!");
                logger.debug("Reading the contents of the old join key from the cache !!!");
            }

            if ( existingRecordInInnerJoin != null ) {

                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);

            }

        }
//        else if ( statusCurJoinKey.equalsIgnoreCase("unchanged") ) {
//            logger.debug("#### Case:: unchanged #####");
//            PrimaryKey oldInnerJoinPrimaryKey = ViewMaintenanceUtilities.createOldJoinKeyfromNewValue(innerJoinPrimaryKeyCur,
//                    deltaTableRecord);
//
//
//            Row oldInnerJoinRecord = ViewMaintenanceUtilities.getExistingRecordIfExists(oldInnerJoinPrimaryKey,
//                    innerJoinTableConfig);
//
//            if (oldInnerJoinRecord == null) {
//
//                logger.debug("#### Case:: unchanged :: The old join key does not exist in the inner join anymore!!!");
//                logger.debug("##### Reading the contents of the old join key!!!");
//            }
//
//
//            if ( existingRecordInInnerJoin != null ) {
//
//                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
//
//            }
//
//        }

    }

    private void intelligentDeletionPreAggViewTable(List<String> userData, List<String> aggregationKeyData,
                                                    String[] baseTableInvolvedArr) {
        if ( userData.get(0).equalsIgnoreCase("sum") ) {
            deleteFromSumPreAggView(getOldAggregationKeyAsPrimaryKey(aggregationKeyData,
                    baseTableInvolvedArr[1]), userData.get(0) + "_" + userData.get(2));
        } else if ( userData.get(0).equalsIgnoreCase("count") ) {
            deleteFromCountPreAggView(getOldAggregationKeyAsPrimaryKey(aggregationKeyData,
                    baseTableInvolvedArr[1]), userData.get(0) + "_" + userData.get(2));
        } else if ( userData.get(0).equalsIgnoreCase("max") ) {

        } else if ( userData.get(0).equalsIgnoreCase("min") ) {

        }
    }

    private void intelligentEntryPreAggViewTable(Row existingRecordPreAggTable, PrimaryKey preAggTablePK, List<String> userData) {
        if ( existingRecordPreAggTable != null ) {

            logger.debug("### Existing record in preAggregateView :: " + existingRecordPreAggTable);
            // Update Agg function column for preagg view table
            if ( userData.get(0).equalsIgnoreCase("sum") ) {
                updateSumPreAggViewOnlyAdding(preAggTablePK, existingRecordPreAggTable, userData);
            } else if ( userData.get(0).equalsIgnoreCase("count") ) {
                updateCountPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
            } else if ( userData.get(0).equalsIgnoreCase("max") ) {
//                        updateMaxPreAggView();
            } else if ( userData.get(0).equalsIgnoreCase("min") ) {

            }
        } else {
            logger.debug("### Fresh entry for record in preAggregateView!!! userData::  " + userData);
            // Insert into Agg function column for preagg view table
            if ( userData.get(0).equalsIgnoreCase("sum") ) {
                insertIntoSumPreAggView(preAggTablePK, userData);
            } else if ( userData.get(0).equalsIgnoreCase("count") ) {
                insertIntoCountPreAggView(preAggTablePK, userData.get(0) + "_" + userData.get(2));
            } else if ( userData.get(0).equalsIgnoreCase("max") ) {

            } else if ( userData.get(0).equalsIgnoreCase("min") ) {

            }
        }
    }

    @Override
    public boolean updateTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public boolean deleteTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    private void updateSumPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable, List<String> userData) {
        String modifiedColumnName = userData.get(0) + "_" + userData.get(2);
        int existingVal = existingRecordPreAggTable.getInt(modifiedColumnName);
        int oldValue = deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.LAST);
        int newValue = 0;

        // Getting the status of the aggregate key change
        List<String> aggregationKeyData = new ArrayList<>();

        aggregationKeyData.add(preAggTablePK.getColumnName().substring(preAggTablePK.getColumnName().indexOf("_") + 1));
        aggregationKeyData.add(preAggTablePK.getColumnInternalCassType());
        String statusEntryColAggKey = ViewMaintenanceUtilities.checkForChangeInAggregationKeyInDeltaView(
                aggregationKeyData, deltaTableRecord);

        if ( statusEntryColAggKey.equalsIgnoreCase("unchanged") ) {
            // If there is no key change then update involves subtraction of the old value and addition of the new
            logger.debug("### updateSumPreAggView | unchanged !!!");
            newValue = (existingVal - oldValue) + Integer.parseInt(userData.get(1));
        } else {
            // If there is a key change or new addition then update involves just addition

            logger.debug("### updateSumPreAggView | not unchanged !!!");
            newValue = existingVal + Integer.parseInt(userData.get(1));
        }


        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Statement updateSumQuery = null;
        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateSumQuery = assignments.and(QueryBuilder.set(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), Integer.parseInt(preAggTablePK.getColumnValueInString())));

        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateSumQuery = assignments.and(QueryBuilder.set(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), preAggTablePK.getColumnValueInString()));
        }

        logger.debug("### UpdateSumQuery in preagg :: " + updateSumQuery);

        CassandraClientUtilities.commandExecution("localhost", updateSumQuery);

    }


    private void updateSumPreAggViewOnlyAdding(PrimaryKey preAggTablePK, Row existingRecordPreAggTable, List<String> userData) {
        String modifiedColumnName = userData.get(0) + "_" + userData.get(2);

        // Get the existing record now from the pre agg table
        Statement existingRecordQueryInPreAggTable = null;

        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            existingRecordQueryInPreAggTable = QueryBuilder.select().all().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(preAggTablePK.getColumnName(),
                    Integer.parseInt(preAggTablePK.getColumnValueInString())));
        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            existingRecordQueryInPreAggTable = QueryBuilder.select().all().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(preAggTablePK.getColumnName(), preAggTablePK.getColumnValueInString()));
        }

        List<Row> existingRecordPreAggTableLatest = CassandraClientUtilities.commandExecution("localhost", existingRecordQueryInPreAggTable);


        int existingVal = existingRecordPreAggTableLatest.get(0).getInt(modifiedColumnName);
//        int existingVal = existingRecordPreAggTable.getInt(modifiedColumnName);
        int oldValue = deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.LAST);
        int newValue = 0;

        // Getting the status of the aggregate key change
        List<String> aggregationKeyData = new ArrayList<>(); // Column_name, CassandraInternalType
        aggregationKeyData.add(preAggTablePK.getColumnName().substring(preAggTablePK.getColumnName().indexOf("_") + 1));
        aggregationKeyData.add(preAggTablePK.getColumnInternalCassType());
//        String statusEntryColAggKey = ViewMaintenanceUtilities.checkForChangeInAggregationKeyInDeltaView(
//                aggregationKeyData, deltaTableRecord);

        // If there is a key change or new addition then update involves just addition

        logger.debug("### Existing value :: {} is {}", modifiedColumnName, existingVal);
        newValue = existingVal + Integer.parseInt(userData.get(1));

        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Statement updateSumQuery = null;
        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateSumQuery = assignments.and(QueryBuilder.set(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), Integer.parseInt(preAggTablePK.getColumnValueInString())));

        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateSumQuery = assignments.and(QueryBuilder.set(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), preAggTablePK.getColumnValueInString()));
        }

        logger.debug("### UpdateSumQuery in preagg :: " + updateSumQuery);

        CassandraClientUtilities.commandExecution("localhost", updateSumQuery);

    }


    private void updateCountPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable, List<String> userData) {
        String modifiedColumnName = userData.get(0) + "_" + userData.get(2);
        int existingVal = existingRecordPreAggTable.getInt(modifiedColumnName);
        int newValue = existingVal + 1;
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Statement updateCountQuery = null;
        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateCountQuery = assignments.and(QueryBuilder.set(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), Integer.parseInt(preAggTablePK.getColumnValueInString())));

        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateCountQuery = assignments.and(QueryBuilder.set(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), preAggTablePK.getColumnValueInString()));
        }

        logger.debug("### UpdateCountQuery :: " + updateCountQuery);

        CassandraClientUtilities.commandExecution("localhost", updateCountQuery);

    }

    private void updateMaxPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable) {

    }

    private void updateMinPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable) {

    }

    private void insertIntoSumPreAggView(PrimaryKey preAggTablePK, List<String> userData) {

        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();

        columnNames.add(preAggTablePK.getColumnName());
        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            objects.add(Integer.parseInt(preAggTablePK.getColumnValueInString()));
        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            objects.add(preAggTablePK.getColumnValueInString());
        }

        columnNames.add(userData.get(0) + "_" + userData.get(2));
        objects.add(Integer.parseInt(userData.get(1)));

        Statement insertSumQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).values(columnNames.toArray(new String[columnNames.size()]),
                objects.toArray());

        logger.debug("### Insert query for Sum into pre agg view table :: " + insertSumQuery);

        CassandraClientUtilities.commandExecution("localhost", insertSumQuery);

    }

    private void insertIntoCountPreAggView(PrimaryKey preAggTablePK, String targetColName) {

        List<String> colNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        colNames.add(preAggTablePK.getColumnName());
        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            objects.add(Integer.parseInt(preAggTablePK.getColumnValueInString()));
        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            objects.add(preAggTablePK.getColumnValueInString());
        }

        colNames.add(targetColName);
        objects.add(1);


        Statement insertCountQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).values(colNames.toArray(new String[colNames.size()]),
                objects.toArray());

        logger.debug("### insert Count query to pre agg view:: " + insertCountQuery);

        CassandraClientUtilities.commandExecution("localhost", insertCountQuery);

    }

    private void insertIntoMaxPreAggView(PrimaryKey preAggTablePK) {

    }

    private void insertIntoMinPreAggView(PrimaryKey preAggTablePK) {

    }

    private void deleteFromSumPreAggView(PrimaryKey oldAggregateKey, String targetColName) {
        Statement updateQuery = null;

        Row existingRecordOldAggKey = ViewMaintenanceUtilities.getExistingRecordIfExists(oldAggregateKey,
                operationViewTables.get(0));
        logger.debug("### Existing record for OldAggKey :: " + existingRecordOldAggKey);
        logger.debug("### Checking -- target column name :: " + targetColName);
        if ( existingRecordOldAggKey == null ) {
            return;
        }
        int oldAggValue = existingRecordOldAggKey.getInt(targetColName);
        int subtractionAmount = deltaTableRecord.getInt(targetColName.split("_")[1] + DeltaViewTrigger.LAST);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with(QueryBuilder.set(targetColName,
                (oldAggValue - subtractionAmount)));
        if ( oldAggregateKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateQuery = assignments.where(QueryBuilder.eq(oldAggregateKey.getColumnName(),
                    Integer.parseInt(oldAggregateKey.getColumnValueInString())));
        } else if ( oldAggregateKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateQuery = assignments.where(QueryBuilder.eq(oldAggregateKey.getColumnName(),
                    oldAggregateKey.getColumnValueInString()));
        }

        logger.debug("### Delete query for sum in preagg View :: " + updateQuery);
        CassandraClientUtilities.commandExecution("localhost", updateQuery);
    }

    private void deleteFromCountPreAggView(PrimaryKey oldAggregateKey, String targetColName) {
        Statement deleteCountQuery = null;

        Row existingRecordOldAggKey = ViewMaintenanceUtilities.getExistingRecordIfExists(oldAggregateKey,
                operationViewTables.get(0));
        logger.debug("### Existing record for OldAggKey :: " + existingRecordOldAggKey);
        int oldAggValue = existingRecordOldAggKey.getInt(targetColName);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with(QueryBuilder.set(targetColName,
                (oldAggValue - 1)));
        if ( oldAggregateKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            deleteCountQuery = assignments.where(QueryBuilder.eq(oldAggregateKey.getColumnName(),
                    Integer.parseInt(oldAggregateKey.getColumnValueInString())));
        } else if ( oldAggregateKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            deleteCountQuery = assignments.where(QueryBuilder.eq(oldAggregateKey.getColumnName(),
                    oldAggregateKey.getColumnValueInString()));
        }

        logger.debug("### Delete query for sum in preagg View :: " + deleteCountQuery);
        CassandraClientUtilities.commandExecution("localhost", deleteCountQuery);
    }


    private String checkForChangeInTargetColValue(List<String> userData) {
        String result = "";
        if ( deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.CURRENT) ==
                deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.LAST) ) {
            result = "unchanged";
        } else {
            if ( deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.LAST) == 0 ) {
                result = "new";
            } else {
                result = "changed";
            }
        }

        logger.debug("### Result for checkForChangeInTargetColValue :: " + result);
        return result;
    }

    private PrimaryKey getOldAggregationKeyAsPrimaryKey(List<String> aggregationKeyData, String baseTableName) {
        PrimaryKey oldAggKey = null;
        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("Integer") ) {
            oldAggKey = new PrimaryKey(baseTableName + "_" + aggregationKeyData.get(0),
                    aggregationKeyData.get(1), deltaTableRecord.getInt(
                    aggregationKeyData.get(0) + DeltaViewTrigger.LAST) + "");
        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("String") ) {
            oldAggKey = new PrimaryKey(baseTableName + "_" + aggregationKeyData.get(0),
                    aggregationKeyData.get(1), deltaTableRecord.getString(
                    aggregationKeyData.get(0) + DeltaViewTrigger.LAST));
        }
        logger.debug("#### Old Aggregate Key as Primary Key :: " + oldAggKey);
        return oldAggKey;
    }

    @Override
    public String toString() {
        return "PreAggOperation{" +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }


    private UtilityProcessor utilityProcessor(TriggerRequest triggerRequest) {
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        List<String> userData = new ArrayList<>(); // For target column: FunctionName, Value, TargetColumnName
        List<String> aggregationKeyData = new ArrayList<>(); // For curr aggregation key: columnName, Cass Type
        PrimaryKey preAggTablePK = null;
        PrimaryKey baseTablePK = null;
        Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                triggerRequest.getBaseTableKeySpace(), triggerRequest.getBaseTableName());

        for ( Column column : operationViewTables.get(0).getColumns() ) {
            String derivedColumnName = column.getName().substring(column.getName().indexOf("_") + 1);
            String prefixForColName = column.getName().substring(0, column.getName().indexOf("_"));
            if ( AVAILABLE_FUNCS.contains(prefixForColName) ) {
                userData.add(prefixForColName);
                userData.add(((String) dataJson.get(derivedColumnName)).replaceAll("'", ""));
                userData.add(derivedColumnName);
            } else {

                for ( Map.Entry<String, ColumnDefinition> columnDefBaseTableEntry : baseTableDesc.entrySet() ) {
                    if ( derivedColumnName.equalsIgnoreCase(columnDefBaseTableEntry.getKey()) ) {

                        aggregationKeyData.add(derivedColumnName);
                        aggregationKeyData.add(columnDefBaseTableEntry.getValue().type.toString());
                        preAggTablePK = new PrimaryKey(column.getName(), columnDefBaseTableEntry.getValue().type.toString(),
                                ((String) dataJson.get(derivedColumnName)).replaceAll("'", ""));
                    }

                    if ( columnDefBaseTableEntry.getValue().isPartitionKey() ) {
                        baseTablePK = new PrimaryKey(columnDefBaseTableEntry.getKey(), columnDefBaseTableEntry
                                .getValue().type.toString(), ((String) dataJson.get(columnDefBaseTableEntry.getKey()))
                                .replaceAll("'", ""));
                    }
                }
            }
        }
        UtilityProcessor processor = new UtilityProcessor();
        processor.setAggregationKeyData(aggregationKeyData);
        processor.setUserData(userData);
        processor.setPreAggTablePK(preAggTablePK);
        processor.setBaseTablePK(baseTablePK);

        logger.debug("##### Validation of the variables for UtilityProcessor object ");
        logger.debug("" + processor);
        logger.debug("##################");

        return processor;
    }

}
