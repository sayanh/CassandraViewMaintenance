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
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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


                            updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);

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


        if ( utilityProcessor != null ) {
            logger.debug("#### Case:: DataJson table is same as the table where aggregate operation needs to be performed!");
            List<String> userData = utilityProcessor.userData; // For target column: FunctionName, Value, TargetColumnName
            List<String> aggregationKeyData = utilityProcessor.aggregationKeyData; // For curr aggregation key: columnName, Cass Type
            PrimaryKey preAggTablePK = utilityProcessor.preAggTablePK;
            PrimaryKey baseTablePK = utilityProcessor.baseTablePK;

            Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                    operationViewTables.get(0));

            Row existingRecordInInnerJoin = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKeyCur,
                    innerJoinTableConfig);

            logger.debug("#### Checking :: existingRecordInInnerJoin :: " + existingRecordInInnerJoin);

            if ( statusCurJoinKey.equalsIgnoreCase("new") ) {

                if ( checkForAggregateTableIsSameAsDataJsonTable(triggerRequest) ) {

                    logger.debug("#### Checking when checkForAggregateTableIsSameAsDataJsonTable true");
                    if ( existingRecordInInnerJoin != null ) {
                        logger.debug("#### Case:: new ::: Update/Insert the aggregate key ");
                        intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
                    } else {
                        logger.debug("#### Case :: new ::: Inner join record does not exist hence no need to maintain preagg!!!");
                    }
                }


            } else if ( statusCurJoinKey.equalsIgnoreCase("changed") || statusCurJoinKey.equalsIgnoreCase("unchanged") ) {
                logger.debug("#### Case:: joinkey changed / unchanged  #####");

                PrimaryKey oldInnerJoinPrimaryKey = ViewMaintenanceUtilities.createOldJoinKeyfromNewValue(innerJoinPrimaryKeyCur,
                        deltaTableRecord);

                boolean curDataPresentInInnerJoin = false;

                Row oldInnerJoinRecord = ViewMaintenanceUtilities.getExistingRecordIfExists(oldInnerJoinPrimaryKey,
                        innerJoinTableConfig);

                logger.debug("#### oldInnerJoinRecord :: " + oldInnerJoinRecord);

                if ( oldInnerJoinRecord == null ) {

                    logger.debug("#### Case:: changed :: The old join key does not exist in the inner join anymore!!!");
                    logger.debug("Reading the contents of the old join key from the cache !!!");

                    Table cacheViewConfig = new Table();
                    cacheViewConfig.setName(innerJoinTableConfig.getName().replace("inner", "innercache"));
                    cacheViewConfig.setKeySpace(innerJoinTableConfig.getKeySpace());
                    cacheViewConfig.setColumns(innerJoinTableConfig.getColumns());


                    Row existingRecordInCache = ViewMaintenanceUtilities.getExistingRecordIfExists(oldInnerJoinPrimaryKey,
                            cacheViewConfig);

                    logger.debug("##### existingRecordInCache :: " + existingRecordInCache);

                    if ( existingRecordInCache != null ) {

                        logger.debug("#### There is an existing record in cache :: " + existingRecordInCache);
                        logger.debug("#### Going for processing of old join keys !! ");
                        processOldJoinKeyAggregateValue(existingRecordInCache, userData, baseTablePK,
                                triggerRequest.getBaseTableName(), preAggTablePK);
                    }

                } else if ( oldInnerJoinRecord != null && statusCurJoinKey.equalsIgnoreCase("unchanged") ) {

                }

                if ( existingRecordInInnerJoin != null ) {

                    // Intelligent deletion of the old agg value
                    // This is true for changed and unchanged status of the aggregate keys

                    curDataPresentInInnerJoin = ViewMaintenanceUtilities.
                            isDataPresentInInnerJoinRecord(baseTablePK, existingRecordInInnerJoin, triggerRequest
                                    .getBaseTableName());

                    if (curDataPresentInInnerJoin) {

                        intelligentDeletionPreAggViewTable(userData, aggregationKeyData, ViewMaintenanceUtilities
                                .getKeyspaceAndTableNameInAnArray(triggerRequest.getBaseTableKeySpace() + "." +
                                        triggerRequest.getBaseTableName()));

                        intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
                    }

                } else if ( oldInnerJoinRecord != null && existingRecordInInnerJoin == null ) {

                    logger.debug("### Case:: when old is still there but the new join key is not !!! ");
                    intelligentDeletionPreAggViewTable(userData, aggregationKeyData, ViewMaintenanceUtilities
                            .getKeyspaceAndTableNameInAnArray(triggerRequest.getBaseTableKeySpace() + "." +
                                    triggerRequest.getBaseTableName()));
                }

            }
        } else {
            logger.debug("##### utilityProcessor is null as table in datajson is not the same as table in triggerrequest");
            if ( statusCurJoinKey.equalsIgnoreCase("new") ) {
                checkAndPreprocessWhenTablesDifferAndTargetTableIsNotTheInput(triggerRequest, innerJoinPrimaryKeyCur);
            } else if ( statusCurJoinKey.equalsIgnoreCase("changed") || statusCurJoinKey.equalsIgnoreCase("unchanged") ) {
                logger.debug("#### Case:: joinkey changed / unchanged  #####");
                PrimaryKey oldInnerJoinPrimaryKey = ViewMaintenanceUtilities.createOldJoinKeyfromNewValue(innerJoinPrimaryKeyCur,
                        deltaTableRecord);


                Row oldInnerJoinRecord = ViewMaintenanceUtilities.getExistingRecordIfExists(oldInnerJoinPrimaryKey,
                        innerJoinTableConfig);

                if ( oldInnerJoinRecord == null ) {

                    logger.debug("#### Case:: changed :: The old join key does not exist in the inner join anymore!!!");
                    logger.debug("##### Reading the contents of the old join key from the cache !!!");

                    Table cacheViewConfig = new Table();
                    cacheViewConfig.setName(innerJoinTableConfig.getName().replace("inner", "innercache"));
                    cacheViewConfig.setKeySpace(innerJoinTableConfig.getKeySpace());
                    cacheViewConfig.setColumns(innerJoinTableConfig.getColumns());


                    Row existingRecordInCache = ViewMaintenanceUtilities.getExistingRecordIfExists(oldInnerJoinPrimaryKey,
                            cacheViewConfig);

                    UtilityProcessor processor = utilityProcessorWhenTableIsDiffFromTargetAggTable(triggerRequest);
                    if ( existingRecordInCache != null ) {
                        logger.debug("#### There is an existing record in cache :: " + existingRecordInCache);
                        logger.debug("#### Going for processing of old join keys !! ");

                        processOldJoinKeyAggregateValue(existingRecordInCache, processor.userData, processor.baseTablePK,
                                triggerRequest.getBaseTableName(), processor.preAggTablePK);
                    }
                }

                checkAndPreprocessWhenTablesDifferAndTargetTableIsNotTheInput(triggerRequest, innerJoinPrimaryKeyCur);


            }


        }

    }



    /**
     * When input happens for other table(say Y) on which aggregate needs not be calculated then view maintenance
     * is only required when the map size of a column in Y is 1. This means this is the first time the
     * record in the reverse join qualified for the inner join hence we should calculate the aggregate for corresponding data
     * in the target base table(say X: i.e. table mentioned in the where clause.)
     * <p/>
     * If the size of the map in Y is more than 1 then view maintenance needs not be performed. As values in X are already
     * maintained by then.
     **/

    private void checkAndPreprocessWhenTablesDifferAndTargetTableIsNotTheInput(TriggerRequest triggerRequest,
                                                                               PrimaryKey innerJoinPrimaryKeyCur) {

        logger.debug("#### Inside checkAndPreprocessWhenTablesDifferAndTargetTableIsNotTheInput");
        Table innerJoinTableConfig = inputViewTables.get(0);

        LinkedTreeMap dataJson = triggerRequest.getDataJson();

        Map<String, ColumnDefinition> innerJoinTableDesc = ViewMaintenanceUtilities.getTableDefinitition(innerJoinTableConfig.getKeySpace(),
                innerJoinTableConfig.getName());
        Row existingRecordInInnerJoin = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKeyCur,
                innerJoinTableConfig);

        if ( existingRecordInInnerJoin != null ) {
            Set keySet = dataJson.keySet();
            Iterator dataIter = keySet.iterator();

            while ( dataIter.hasNext() ) {
                String tempDataKey = (String) dataIter.next();
                if ( (innerJoinTableDesc.containsKey(tempDataKey) && !innerJoinTableDesc.get(tempDataKey).isPartitionKey())
                        || innerJoinTableDesc.containsKey(triggerRequest.getBaseTableName() + "_" +
                        tempDataKey) && !innerJoinTableDesc.get(triggerRequest.getBaseTableName() + "_" +
                        tempDataKey).isPartitionKey() ) {

                    ColumnDefinition currentColumn = innerJoinTableDesc.get(tempDataKey);
                    if ( currentColumn == null ) {
                        currentColumn = innerJoinTableDesc.get(triggerRequest.getBaseTableName() + "_" + tempDataKey);
                    }
                    String dataType = ViewMaintenanceUtilities.getCQL3DataTypeFromCassandraInternalDataType(
                            currentColumn.type.toString());
                    if ( dataType.equalsIgnoreCase("map <int, text>") ) {

                        Map<Integer, String> mapFromInnerJoin = null;
                        try {
                            mapFromInnerJoin = existingRecordInInnerJoin.getMap(
                                    tempDataKey, Integer.class, String.class);
                        } catch ( IllegalArgumentException ille ) {

                            logger.error(" Error as the the column did not exactly match as it contains the table " +
                                    "name as a prefix!! " + ille.getMessage());
                            if ( mapFromInnerJoin == null ) {
                                mapFromInnerJoin = existingRecordInInnerJoin.getMap(
                                        triggerRequest.getBaseTableName() + "_" + tempDataKey, Integer.class,
                                        String.class);

                                logger.debug("#### mapFromInnerJoin :: " + mapFromInnerJoin);
                            }
                        }


                        if ( mapFromInnerJoin.size() == 1 ) {
                            logger.debug("#### Handling of the target column needs to be done!!");
                            processTargetColumnWhenTriggerReqTableDoesNotMatchTargetTable(triggerRequest,
                                    existingRecordInInnerJoin);
                        } else {
                            logger.debug("#### Non target column table entry and not the first one " +
                                    "| nothing needs to be done");

                        }

                    }
                }
            }
        }

    }

    private void processTargetColumnWhenTriggerReqTableDoesNotMatchTargetTable(TriggerRequest triggerRequest,
                                                                               Row existingRecordInInnerJoin) {

        Table preAggViewConfig = operationViewTables.get(0);
//        Table innerJoinTableConfig = inputViewTables.get(0);
        String derivedColumnName = null;
        String prefixForColName = null;
        PrimaryKey baseTablePrimaryKey = null;
        Map<?, ?> mapAggKey = null;
        Map<?, Integer> mapAggVal = null;
        String baseTableName = null;


        // Computing the mapAggKey begins ####################

        for ( Column column : preAggViewConfig.getColumns() ) {

            logger.debug("#### Checking executing column :: " + column);

            if ( column.isPrimaryKey() ) {
                baseTableName = column.getName().substring(0, column.getName().indexOf("_"));
                String aggregateColumnName = column.getName().substring(column.getName().indexOf("_") + 1);

                baseTablePrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableDescWithoutValue(
                        ViewMaintenanceUtilities.getTableDefinitition(triggerRequest.getBaseTableKeySpace(),
                                baseTableName));


                logger.debug("#### processTargetColumnWhenTriggerReqTableDoesNotMatchTargetTable | baseTablePrimaryKey "
                        + baseTablePrimaryKey);


                // Computing the sum or count hence it is always Integer type
                if ( column.getJavaDataType().equalsIgnoreCase("Integer") ) {
                    if ( baseTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                        try {
                            mapAggKey = existingRecordInInnerJoin.getMap(aggregateColumnName,
                                    Integer.class, Integer.class);
                        } catch ( IllegalArgumentException ille ) {

                            logger.error("### Error illegal argument exception :: " + ille.getMessage());
                            if ( mapAggKey == null ) {
                                mapAggKey = existingRecordInInnerJoin.getMap(column.getName(), Integer.class, Integer.class);
                            }
                        }


                    } else if ( baseTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                        try {
                            mapAggKey = existingRecordInInnerJoin.getMap(aggregateColumnName,
                                    String.class, Integer.class);
                        } catch ( IllegalArgumentException ille ) {

                            logger.error("### Error illegal argument exception :: " + ille.getMessage());
                            if ( mapAggKey == null ) {
                                mapAggKey = existingRecordInInnerJoin.getMap(column.getName(), String.class, Integer.class);
                            }
                        }

                    }
                } else if ( column.getJavaDataType().equalsIgnoreCase("String") ) {
                    if ( baseTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                        try {
                            mapAggKey = existingRecordInInnerJoin.getMap(aggregateColumnName,
                                    Integer.class, Integer.class);
                        } catch ( IllegalArgumentException ille ) {

                            logger.error("### Error illegal argument exception :: " + ille.getMessage());
                            if ( mapAggKey == null ) {
                                mapAggKey = existingRecordInInnerJoin.getMap(column.getName(), Integer.class, String.class);
                            }
                        }


                    } else if ( baseTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                        try {
                            mapAggKey = existingRecordInInnerJoin.getMap(aggregateColumnName,
                                    String.class, Integer.class);
                        } catch ( IllegalArgumentException ille ) {

                            logger.error("### Error illegal argument exception :: " + ille.getMessage());
                            if ( mapAggKey == null ) {
                                mapAggKey = existingRecordInInnerJoin.getMap(column.getName(), String.class, String.class);
                            }
                        }
                    }
                }


                break;
            }
        }

        logger.debug("#### mapAggKey calculated as :: " + mapAggKey);

        // Computing the mapAggKey ends ####################

        // Computing the mapAggVal begins ####################

        for ( Column column : preAggViewConfig.getColumns() ) {

            if ( !column.isPrimaryKey() ) {
                derivedColumnName = column.getName().substring(column.getName().indexOf("_") + 1);
                prefixForColName = column.getName().substring(0, column.getName().indexOf("_"));
                if ( baseTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    try {
                        mapAggVal = existingRecordInInnerJoin.getMap(derivedColumnName, Integer.class, Integer.class);
                    } catch ( IllegalArgumentException ille ) {
                        logger.error("### Error illegal argument exception :: " + ille.getMessage());
                        if ( mapAggVal != null ) {
                            mapAggVal = existingRecordInInnerJoin.getMap(baseTableName + "_" + derivedColumnName, Integer.class, Integer.class);
                        }
                    }

                } else if ( baseTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                    try {
                        mapAggVal = existingRecordInInnerJoin.getMap(derivedColumnName, String.class, Integer.class);
                    } catch ( IllegalArgumentException ille ) {
                        logger.error("### Error illegal argument exception :: " + ille.getMessage());
                        if ( mapAggVal != null ) {
                            mapAggVal = existingRecordInInnerJoin.getMap(baseTableName + "_" + derivedColumnName, String.class, Integer.class);
                        }
                    }
                }
                break;
            }

        }

        logger.debug("#### mapAggVal calculated as :: " + mapAggVal);

        combineAggrValueAndKey(mapAggKey, mapAggVal, prefixForColName, derivedColumnName);

    }

    private void combineAggrValueAndKey(Map<?, ?> mapAggKey, Map<?, Integer> mapAggVal, String functionName, String aggColumnName) {

        logger.debug("#### mapAggKey :: " + mapAggKey);
        logger.debug("#### mapAggVal :: " + mapAggVal);

        PrimaryKey primaryKeyPreAggTable = ViewMaintenanceUtilities.getPrimaryKeyFromTableDescWithoutValue(
                ViewMaintenanceUtilities.getTableDefinitition(operationViewTables.get(0).getKeySpace(),
                        operationViewTables.get(0).getName()));
        Row exisitingRecordPreAgg = null;
        List<String> userData = new ArrayList<>(); // Format: <function_involved>, <valueInString>, <targetColumnName>
        for ( Map.Entry<?, ?> keyEntry : mapAggKey.entrySet() ) {
            int value = mapAggVal.get(keyEntry.getKey());
            if ( keyEntry.getValue() instanceof Integer ) {
                primaryKeyPreAggTable.setColumnValueInString((Integer) keyEntry.getValue() + "");
                exisitingRecordPreAgg = ViewMaintenanceUtilities.getExistingRecordIfExists(primaryKeyPreAggTable,
                        operationViewTables.get(0));
            } else if ( keyEntry.getValue() instanceof String ) {
                primaryKeyPreAggTable.setColumnValueInString((String) keyEntry.getValue());
                exisitingRecordPreAgg = ViewMaintenanceUtilities.getExistingRecordIfExists(primaryKeyPreAggTable,
                        operationViewTables.get(0));
            }
            userData.add(functionName);
            userData.add(value + "");
            userData.add(aggColumnName);
            intelligentEntryPreAggViewTable(exisitingRecordPreAgg, primaryKeyPreAggTable, userData);
            userData.clear();
        }
    }


    private void processOldJoinKeyAggregateValue(Row existingRowCacheTable, List<String> userData, PrimaryKey baseTablePK,
                                                 String baseTableName, PrimaryKey preAggTablePK) {

        String functionInvolved = userData.get(0);
        String targetAggColumn = userData.get(2);


        if ( baseTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {


            Map<Integer, Integer> aggValueMap = existingRowCacheTable.getMap(targetAggColumn, Integer.class, Integer.class);

            if ( aggValueMap == null || aggValueMap.isEmpty() ) {
                aggValueMap = existingRowCacheTable.getMap(baseTableName + "_" + targetAggColumn, Integer.class, Integer.class);
            }

            logger.debug("#### Values Map :: cacheTable Contents :: " + aggValueMap);

            if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                Map<Integer, Integer> finalMap = new HashMap<>();
                Map<Integer, Integer> aggKeyMap = null;
                aggKeyMap = existingRowCacheTable.getMap(preAggTablePK.getColumnName().split("_")[1], Integer.class, Integer.class);

                if ( aggKeyMap == null || aggKeyMap.isEmpty() ) {
                    aggKeyMap = existingRowCacheTable.getMap(preAggTablePK.getColumnName(), Integer.class, Integer.class);
                }

//                if (aggKeyMap == null) {
//                    logger.debug("##### Return... as the old join key was never a part of the inner join");
//                    return;
//                }

                logger.debug("#### AggKeyMap :: " + aggKeyMap);
                // Traversing the Key-Map and matching it to the value Map

                for ( Map.Entry<Integer, Integer> aggKeyMapEntry : aggKeyMap.entrySet() ) {
                    int baseTablePrimaryKey = aggKeyMapEntry.getKey();
                    if ( finalMap.containsKey(aggKeyMapEntry.getValue()) ) {
                        if ( functionInvolved.equalsIgnoreCase("sum") ) {
                            int alreadyExisting = finalMap.get(aggKeyMapEntry.getValue()) + aggValueMap.get(baseTablePrimaryKey);
                            finalMap.put(aggKeyMapEntry.getValue(), alreadyExisting);
                        } else if ( functionInvolved.equalsIgnoreCase("count") ) {
                            int alreadyExisting = finalMap.get(aggKeyMapEntry.getValue()) + 1;
                            finalMap.put(aggKeyMapEntry.getValue(), alreadyExisting);
                        }

                    } else {
                        finalMap.put(aggKeyMapEntry.getValue(), aggValueMap.get(baseTablePrimaryKey));
                    }

                }

                logger.debug("#### Final Map to process aggregates :: " + finalMap);

                deleteEachElementFromOldJoinKeyMap(finalMap, userData, preAggTablePK);

                clearCacheTable();

            } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
                Map<String, Integer> finalMap = new HashMap<>();
                Map<Integer, String> aggKeyMap = null;
                try {
                    aggKeyMap = existingRowCacheTable.getMap(preAggTablePK.getColumnName().split("_")[1], Integer.class, String.class);
                } catch ( IllegalArgumentException ille ) {
                    logger.error("### Error illegal argument exception :: " + ille.getMessage());
                    if ( aggKeyMap == null || aggKeyMap.isEmpty() ) {
                        aggKeyMap = existingRowCacheTable.getMap(preAggTablePK.getColumnName(), Integer.class, String.class);
                    }
                }


                logger.debug("#### AggKeyMap :: " + aggKeyMap);

                for ( Map.Entry<Integer, String> aggKeyMapEntry : aggKeyMap.entrySet() ) {
                    int baseTablePrimaryKey = aggKeyMapEntry.getKey();
                    if ( finalMap.containsKey(aggKeyMapEntry.getValue()) ) {
                        if ( functionInvolved.equalsIgnoreCase("sum") ) {
                            int alreadyExisting = finalMap.get(aggKeyMapEntry.getValue()) + aggValueMap.get(baseTablePrimaryKey);
                            finalMap.put(aggKeyMapEntry.getValue(), alreadyExisting);
                        } else if ( functionInvolved.equalsIgnoreCase("count") ) {
                            int alreadyExisting = finalMap.get(aggKeyMapEntry.getValue()) + 1;
                            finalMap.put(aggKeyMapEntry.getValue(), alreadyExisting);
                        }

                    } else {
                        finalMap.put(aggKeyMapEntry.getValue(), aggValueMap.get(baseTablePrimaryKey));
                    }
                }

                logger.debug("#### Final Map to process aggregates :: " + finalMap);

                deleteEachElementFromOldJoinKeyMap(finalMap, userData, preAggTablePK);

                clearCacheTable();

            }


        } else if ( baseTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {

//            Map<String, Integer> valuesMap = existingRowCacheTable.getMap(targetAggColumn, String.class, Integer.class);

        }

    }

    private void clearCacheTable() {
        String cacheTableName = inputViewTables.get(0).getName().replace("inner", "innercache");
        Map<String, ColumnDefinition> cacheTableDesc = ViewMaintenanceUtilities.getTableDefinitition(inputViewTables.
                get(0).getKeySpace(), cacheTableName);
        PrimaryKey cacheTablePK = null;
        for ( Map.Entry<String, ColumnDefinition> cacheColEntry : cacheTableDesc.entrySet() ) {
            if ( cacheColEntry.getValue().isPartitionKey() ) {
                cacheTablePK = new PrimaryKey(cacheColEntry.getKey(), cacheColEntry.getValue().type.toString(), "");
            }
        }

        Statement selectAllStatement = QueryBuilder.select().all().from(inputViewTables.get(0).getKeySpace(), cacheTableName);

        List<Row> selectAllResult = CassandraClientUtilities.commandExecution("localhost", selectAllStatement);

        if ( selectAllResult != null && selectAllResult.size() > 0 ) {
            Statement deleteQueryInCacheTableQuery = null;
            for ( Row cacheTableRow : selectAllResult ) {
                if ( cacheTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    deleteQueryInCacheTableQuery = QueryBuilder.delete().from(inputViewTables.get(0).getKeySpace(),
                            cacheTableName).where(QueryBuilder.eq(cacheTablePK.getColumnName(),
                            cacheTableRow.getInt(cacheTablePK.getColumnName())));
                } else if ( cacheTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
                    deleteQueryInCacheTableQuery = QueryBuilder.delete().from(inputViewTables.get(0).getKeySpace(),
                            cacheTableName).where(QueryBuilder.eq(cacheTablePK.getColumnName(),
                            cacheTableRow.getString(cacheTablePK.getColumnName())));
                }

                logger.debug("### Delete query in cacheTable :: " + deleteQueryInCacheTableQuery);

                CassandraClientUtilities.commandExecution("localhost", deleteQueryInCacheTableQuery);

            }


        }
    }


    private void deleteEachElementFromOldJoinKeyMap(Map<?, Integer> finalMap, List<String> userData, PrimaryKey preAggKeySample) {

        for ( Map.Entry finalMapEntry : finalMap.entrySet() ) {

            PrimaryKey preAggKey = new PrimaryKey(preAggKeySample.getColumnName(), preAggKeySample.getColumnInternalCassType(),
                    finalMapEntry.getKey() + "");
            if ( userData.get(0).equalsIgnoreCase("sum") ) {
                deleteFromSumPreAggView(preAggKey, userData.get(0) + "_" + userData.get(2), (int) finalMapEntry.getValue());
            } else if ( userData.get(0).equalsIgnoreCase("count") ) {
//                deleteFromCountPreAggView(preAggKey, userData.get(0) + "_" + userData.get(2), (int) finalMapEntry.getValue());
                //TODO:: yet to be implemented.
            }
        }
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
        logger.debug("##### Entering delete trigger for PreAggregate Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved :: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record ::  {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure :: {}", this.inputViewTables);
        logger.debug("##### Trigger request :: " + triggerRequest);

        Table preAggTableConfig = operationViewTables.get(0);
        Map<String, ColumnDefinition> preAggTableDesc = ViewMaintenanceUtilities.getTableDefinitition(preAggTableConfig.getKeySpace(),
                preAggTableConfig.getName());


        PrimaryKey actualPrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableConfigWithoutValue(triggerRequest
                .getBaseTableKeySpace(), triggerRequest.getBaseTableName());

        if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            actualPrimaryKey.setColumnValueInString(deltaTableRecord.getInt(actualPrimaryKey.getColumnName()) + "");
        } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            actualPrimaryKey.setColumnValueInString(deltaTableRecord.getString(actualPrimaryKey.getColumnName()));
        }

        PrimaryKey preAggPrimaryKey = null;
        String targetColName = "";
        String functionName = "";
        String actualBaseTableName = "";

        for ( Map.Entry<String, ColumnDefinition> preAggColEntry : preAggTableDesc.entrySet() ) {
            if ( preAggColEntry.getValue().isPartitionKey() ) {
                String derivedColumnName = preAggColEntry.getKey().substring(preAggColEntry.getKey().indexOf("_") + 1);
                actualBaseTableName = preAggColEntry.getKey().substring(0, preAggColEntry.getKey().indexOf("_"));
                String cql3Type = ViewMaintenanceUtilities.getCQL3DataTypeFromCassandraInternalDataType(preAggColEntry
                        .getValue().type.toString());
                if ( cql3Type.equalsIgnoreCase("text") ) {
                    preAggPrimaryKey = new PrimaryKey(preAggColEntry.getKey(), preAggColEntry.getValue().type.toString(),
                            deltaTableRecord.getString(derivedColumnName + DeltaViewTrigger.CURRENT));

                    logger.debug("#### Pre agg primary key :: " + preAggPrimaryKey);
                } else if ( cql3Type.equalsIgnoreCase("int") ) {
                    preAggPrimaryKey = new PrimaryKey(preAggColEntry.getKey(), preAggColEntry.getValue().type.toString(),
                            deltaTableRecord.getInt(derivedColumnName + DeltaViewTrigger.CURRENT) + "");
                    logger.debug("#### Pre agg primary key :: " + preAggPrimaryKey);
                }

            } else {
                targetColName = preAggColEntry.getKey().substring(preAggColEntry.getKey().indexOf("_") + 1);
                functionName = preAggColEntry.getKey().substring(0, preAggColEntry.getKey().indexOf("_"));
            }
        }


        if ( inputViewTables != null && inputViewTables.get(0).getName().contains(WHERE_TABLE_INDENTIFIER) ) {
            logger.debug("#### deleteTrigger :: entry point where case");

            Table whereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(triggerRequest, inputViewTables);

            boolean didCurrentValueSatisfyWhereClause = ViewMaintenanceUtilities.checkCurrValueSatisfyWhereClause(
                    viewConfig, triggerRequest, deltaTableRecord, whereTable);

            logger.debug("#### didCurrentValueSatisfyWhereClause :: " + didCurrentValueSatisfyWhereClause);

            if ( didCurrentValueSatisfyWhereClause ) {

                if ( functionName.equalsIgnoreCase("sum") ) {
                    deleteCurFromSumPreAggView(preAggPrimaryKey, functionName + "_" + targetColName);
                } else if ( functionName.equalsIgnoreCase("count") ) {
                    deleteFromCountPreAggView(preAggPrimaryKey, functionName + "_" + targetColName);
                }
            }

        } else if ( inputViewTables != null && inputViewTables.get(0).getName().contains(INNER_JOIN_TABLE_INDENTIFIER) ) {

            logger.debug("#### deleteTrigger :: entry point inner join table case");

            Table innerJoinTableConfig = inputViewTables.get(0);

            Map<String, ColumnDefinition> innerJoinTableDesc = ViewMaintenanceUtilities.getTableDefinitition(innerJoinTableConfig.getKeySpace(),
                    innerJoinTableConfig.getName());

            Table cacheViewConfig = new Table();
            cacheViewConfig.setName(innerJoinTableConfig.getName().replace("inner", "innercache"));
            cacheViewConfig.setKeySpace(innerJoinTableConfig.getKeySpace());
            cacheViewConfig.setColumns(innerJoinTableConfig.getColumns());
            Row existingRecordInCache = null;
            Row existingRecordInInnerJoin = null;

            PrimaryKey innerJoinPrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableConfigWithoutValue(
                    innerJoinTableConfig.getKeySpace(), innerJoinTableConfig.getName());

            if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                innerJoinPrimaryKey.setColumnValueInString(deltaTableRecord.getInt(innerJoinPrimaryKey.getColumnName()
                        + DeltaViewTrigger.CURRENT) + "");
            } else if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                innerJoinPrimaryKey.setColumnValueInString(deltaTableRecord.getString(innerJoinPrimaryKey.getColumnName()
                        + DeltaViewTrigger.CURRENT));
            }

            existingRecordInCache = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKey,
                    cacheViewConfig);


            logger.debug("#### exisitingRecordInCache :: " + existingRecordInCache);

            if ( actualBaseTableName.equalsIgnoreCase(triggerRequest.getBaseTableName()) ) {

                logger.debug("#### Update request came for concerned table !!!");

                if ( existingRecordInCache != null ) {

                    deleteTriggerWhenInnerJoinCacheIsPresent(actualPrimaryKey, preAggPrimaryKey,
                            existingRecordInCache, actualBaseTableName,
                            targetColName, functionName);

                    // Delete from inner join cache
                } else {
                    existingRecordInInnerJoin = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKey,
                            innerJoinTableConfig);

                    logger.debug("#### existingRecordInInnerJoin :: " + existingRecordInInnerJoin);

                    if ( existingRecordInInnerJoin != null ) {

                        if ( functionName.equalsIgnoreCase("sum") ) {
                            deleteCurFromSumPreAggView(preAggPrimaryKey, functionName + "_" + targetColName);
                        } else if ( functionName.equalsIgnoreCase("count") ) {
                            deleteFromCountPreAggView(preAggPrimaryKey, functionName + "_" + targetColName);
                        }
                    }

                }
            } else {

                logger.debug("#### Update request came for other table!!");

                if ( existingRecordInCache != null ) {
                    deleteTriggerWhenInnerJoinCacheIsPresent(actualPrimaryKey, preAggPrimaryKey,
                            existingRecordInCache, actualBaseTableName,
                            targetColName, functionName);

                }
            }


        } else {
            logger.debug("#### deleteTrigger :: only agg case");
            int subtractionAmount = deltaTableRecord.getInt(targetColName + DeltaViewTrigger.CURRENT);
            if ( functionName.equalsIgnoreCase("sum") ) {
                deleteFromSumPreAggView(preAggPrimaryKey, functionName + "_" + targetColName, subtractionAmount);
            } else if ( functionName.equalsIgnoreCase("count") ) {
                deleteFromCountPreAggView(preAggPrimaryKey, functionName + "_" + targetColName);
            }

        }
        return true;
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

    private void deleteTriggerWhenInnerJoinCacheIsPresent(PrimaryKey actualPrimaryKey, PrimaryKey preAggPrimaryKey,
                                                          Row existingRecordInCache, String actualBaseTableName,
                                                          String targetColName, String functionName) {

        // Computing Agg Key
        Map<?, ?> aggKeyMap = null;

        if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            if ( preAggPrimaryKey.getColumnJavaType().equals("Integer") ) {
                try {
                    aggKeyMap = existingRecordInCache.getMap(preAggPrimaryKey.getColumnName(), Integer.class,
                            Integer.class);
                } catch ( IllegalArgumentException ille ) {
                    logger.debug("#### Exception!! " + ille.getMessage());
                    aggKeyMap = existingRecordInCache.getMap(actualBaseTableName + "_" +
                            preAggPrimaryKey.getColumnName(), Integer.class, Integer.class);
                }
            } else if ( preAggPrimaryKey.getColumnJavaType().equals("String") ) {
                try {
                    aggKeyMap = existingRecordInCache.getMap(preAggPrimaryKey.getColumnName(), Integer.class,
                            String.class);
                } catch ( IllegalArgumentException ille ) {
                    logger.debug("#### Exception!! " + ille.getMessage());
                    aggKeyMap = existingRecordInCache.getMap(actualBaseTableName + "_" +
                            preAggPrimaryKey.getColumnName(), Integer.class, String.class);
                }
            }
        } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            if ( preAggPrimaryKey.getColumnJavaType().equals("Integer") ) {
                try {
                    aggKeyMap = existingRecordInCache.getMap(preAggPrimaryKey.getColumnName(), String.class,
                            Integer.class);
                } catch ( IllegalArgumentException ille ) {
                    logger.debug("#### Exception!! " + ille.getMessage());
                    aggKeyMap = existingRecordInCache.getMap(actualBaseTableName + "_" +
                            preAggPrimaryKey.getColumnName(), String.class, Integer.class);
                }
            } else if ( preAggPrimaryKey.getColumnJavaType().equals("String") ) {
                try {
                    aggKeyMap = existingRecordInCache.getMap(preAggPrimaryKey.getColumnName(), String.class,
                            String.class);
                } catch ( IllegalArgumentException ille ) {
                    logger.debug("#### Exception!! " + ille.getMessage());
                    aggKeyMap = existingRecordInCache.getMap(actualBaseTableName + "_" +
                            preAggPrimaryKey.getColumnName(), String.class, String.class);
                }
            }
        }


        // Computing AggVal map
        Map<?, Integer> aggValMap = null;

        if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            try {
                aggValMap = existingRecordInCache.getMap(targetColName, Integer.class,
                        Integer.class);
            } catch ( IllegalArgumentException ille ) {
                logger.debug("#### Exception!! " + ille.getMessage());
                aggValMap = existingRecordInCache.getMap(actualBaseTableName + "_" +
                        targetColName, Integer.class, Integer.class);
            }
        } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            try {
                aggValMap = existingRecordInCache.getMap(targetColName, String.class,
                        Integer.class);
            } catch ( IllegalArgumentException ille ) {
                logger.debug("#### Exception!! " + ille.getMessage());
                aggValMap = existingRecordInCache.getMap(actualBaseTableName + "_" +
                        targetColName, String.class, Integer.class);
            }
        }

        for ( Map.Entry<?, ?> aggKeyMapEntry : aggKeyMap.entrySet() ) {
            if ( functionName.equalsIgnoreCase("sum") ) {

                PrimaryKey tempPrimaryKeyPreAgg = null;
                int value = aggValMap.get(aggKeyMapEntry.getKey());
                if ( preAggPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                    tempPrimaryKeyPreAgg = new PrimaryKey(preAggPrimaryKey.getColumnName(),
                            preAggPrimaryKey.getColumnInternalCassType(), aggKeyMapEntry.getKey() + "");

                } else if ( preAggPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {

                    tempPrimaryKeyPreAgg = new PrimaryKey(preAggPrimaryKey.getColumnName(),
                            preAggPrimaryKey.getColumnInternalCassType(), aggKeyMapEntry.getKey() + "");

                }

                deleteFromSumPreAggView(tempPrimaryKeyPreAgg, targetColName, value);

            } else if ( functionName.equalsIgnoreCase("count") ) {
                PrimaryKey tempPrimaryKeyPreAgg = null;
                if ( preAggPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                    tempPrimaryKeyPreAgg = new PrimaryKey(preAggPrimaryKey.getColumnName(),
                            preAggPrimaryKey.getColumnInternalCassType(), aggKeyMapEntry.getKey() + "");

                } else if ( preAggPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {

                    tempPrimaryKeyPreAgg = new PrimaryKey(preAggPrimaryKey.getColumnName(),
                            preAggPrimaryKey.getColumnInternalCassType(), aggKeyMapEntry.getKey() + "");

                }

                deleteFromCountPreAggView(tempPrimaryKeyPreAgg, targetColName);
            }
        }


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
//        int oldValue = deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.LAST);
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
        // TODO: yet to be implemented
    }

    private void insertIntoMinPreAggView(PrimaryKey preAggTablePK) {
        // TODO: yet to be implemented
    }

    private void deleteFromSumPreAggView(PrimaryKey aggregateKey, String targetColName) {
        Statement updateQuery = null;

        Row existingRecordOldAggKey = ViewMaintenanceUtilities.getExistingRecordIfExists(aggregateKey,
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
        if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    Integer.parseInt(aggregateKey.getColumnValueInString())));
        } else if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    aggregateKey.getColumnValueInString()));
        }

        logger.debug("### Delete query for sum in preagg View :: " + updateQuery);
        CassandraClientUtilities.commandExecution("localhost", updateQuery);
    }

    private void deleteCurFromSumPreAggView(PrimaryKey aggregateKey, String targetColName) {
        Statement updateQuery = null;

        Row existingRecordOldAggKey = ViewMaintenanceUtilities.getExistingRecordIfExists(aggregateKey,
                operationViewTables.get(0));
        logger.debug("### Existing record for OldAggKey :: " + existingRecordOldAggKey);
        logger.debug("### Checking -- target column name :: " + targetColName);
        if ( existingRecordOldAggKey == null ) {
            return;
        }
        int oldAggValue = existingRecordOldAggKey.getInt(targetColName);
        int subtractionAmount = deltaTableRecord.getInt(targetColName.split("_")[1] + DeltaViewTrigger.CURRENT);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with(QueryBuilder.set(targetColName,
                (oldAggValue - subtractionAmount)));
        if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    Integer.parseInt(aggregateKey.getColumnValueInString())));
        } else if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    aggregateKey.getColumnValueInString()));
        }

        logger.debug("### Delete query for sum in preagg View :: " + updateQuery);
        CassandraClientUtilities.commandExecution("localhost", updateQuery);
    }

    private void deleteFromSumPreAggView(PrimaryKey aggregateKey, String targetColName, int subtractionAmount) {
        Statement updateQuery = null;

        Row existingRecordOldAggKey = ViewMaintenanceUtilities.getExistingRecordIfExists(aggregateKey,
                operationViewTables.get(0));
        logger.debug("### Existing record for OldAggKey :: " + existingRecordOldAggKey);
        logger.debug("### Checking -- target column name :: " + targetColName);
        if ( existingRecordOldAggKey == null ) {
            return;
        }
        int oldAggValue = existingRecordOldAggKey.getInt(targetColName);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with(QueryBuilder.set(targetColName,
                (oldAggValue - subtractionAmount)));
        if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    Integer.parseInt(aggregateKey.getColumnValueInString())));
        } else if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    aggregateKey.getColumnValueInString()));
        }

        logger.debug("### Delete query for sum in preagg View :: " + updateQuery);
        CassandraClientUtilities.commandExecution("localhost", updateQuery);
    }

    private void deleteFromCountPreAggView(PrimaryKey aggregateKey, String targetColName) {
        Statement deleteCountQuery = null;

        Row existingRecordOldAggKey = ViewMaintenanceUtilities.getExistingRecordIfExists(aggregateKey,
                operationViewTables.get(0));
        logger.debug("### Existing record for aggKey :: " + existingRecordOldAggKey);
        int oldAggValue = existingRecordOldAggKey.getInt(targetColName);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with(QueryBuilder.set(targetColName,
                (oldAggValue - 1)));
        if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            deleteCountQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    Integer.parseInt(aggregateKey.getColumnValueInString())));
        } else if ( aggregateKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            deleteCountQuery = assignments.where(QueryBuilder.eq(aggregateKey.getColumnName(),
                    aggregateKey.getColumnValueInString()));
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


    private UtilityProcessor utilityProcessorWhenTableIsDiffFromTargetAggTable(TriggerRequest triggerRequest) {

        UtilityProcessor processor = null;
        PrimaryKey preAggTablePK = null;
        PrimaryKey baseTablePK = null;
        List<String> userData = null;

        for ( Column columnPreAggTable : operationViewTables.get(0).getColumns() ) {
            String derivedColumnName = columnPreAggTable.getName().substring(columnPreAggTable.getName().indexOf("_") + 1);
            String prefixForColName = columnPreAggTable.getName().substring(0, columnPreAggTable.getName().indexOf("_"));
            if ( columnPreAggTable.isPrimaryKey() ) {

                preAggTablePK = new PrimaryKey(columnPreAggTable.getName(),
                        ViewMaintenanceUtilities.getCassInternalDataTypeFromCQL3DataType(columnPreAggTable.getDataType()),
                        "");

                Map<String, ColumnDefinition> baseTableConfig = ViewMaintenanceUtilities.getTableDefinitition(
                        triggerRequest.getBaseTableKeySpace(), prefixForColName);

                for ( Map.Entry<String, ColumnDefinition> baseTableColEntry : baseTableConfig.entrySet() ) {
                    if ( baseTableColEntry.getValue().isPartitionKey() ) {

                        baseTablePK = new PrimaryKey(baseTableColEntry.getKey(), baseTableColEntry.getValue().type.toString(),
                                "");
                    }
                }


            } else {
                userData = new ArrayList<>();
                userData.add(prefixForColName);
                userData.add("");
                userData.add(derivedColumnName);
            }
        }

        processor = new UtilityProcessor();
        processor.setUserData(userData);
        processor.setBaseTablePK(baseTablePK);
        processor.setPreAggTablePK(preAggTablePK);

        logger.debug("#### Checking utilityProcessorWhenTableIsDiffFromTargetAggTable :: UtilityProcessor obj: " +
                processor);

        return processor;

    }


    private UtilityProcessor utilityProcessor(TriggerRequest triggerRequest) {
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        PrimaryKey preAggTablePK = null;
        PrimaryKey baseTablePK = null;
        Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                triggerRequest.getBaseTableKeySpace(), triggerRequest.getBaseTableName());
        UtilityProcessor processor = null;

        if ( checkForAggregateTableIsSameAsDataJsonTable(triggerRequest) ) {
            List<String> userData = new ArrayList<>(); // For target column: FunctionName, Value, TargetColumnName
            List<String> aggregationKeyData = new ArrayList<>(); // For curr aggregation key: columnName, Cass Type

            for ( Column column : operationViewTables.get(0).getColumns() ) {
                String derivedColumnName = column.getName().substring(column.getName().indexOf("_") + 1);
                String prefixForColName = column.getName().substring(0, column.getName().indexOf("_"));

                logger.debug("#### Checking ... utilityprocessor derivedColumnName {} :: " +
                        "prefixForColName {} ", derivedColumnName, prefixForColName);

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

            processor = new UtilityProcessor();
            processor.setAggregationKeyData(aggregationKeyData);
            processor.setUserData(userData);
            processor.setPreAggTablePK(preAggTablePK);
            processor.setBaseTablePK(baseTablePK);

        }


        logger.debug("##### Validation of the variables for UtilityProcessor object ");
        logger.debug("" + processor);
        logger.debug("##################");

        return processor;
    }


    private boolean checkForAggregateTableIsSameAsDataJsonTable(TriggerRequest triggerRequest) {
        boolean isSame = false;

        Map<String, ColumnDefinition> preAggTableDesc = ViewMaintenanceUtilities.getTableDefinitition(operationViewTables.get(0)
                .getKeySpace(), operationViewTables.get(0).getName());

        for ( Map.Entry<String, ColumnDefinition> preAggTableDescEntry : preAggTableDesc.entrySet() ) {
            if ( preAggTableDescEntry.getValue().isPartitionKey() ) {
//                String derivedColumnName = preAggTableDescEntry.getKey().substring(preAggTableDescEntry.getKey().indexOf("_") + 1);
                String prefixForColName = preAggTableDescEntry.getKey().substring(0, preAggTableDescEntry.getKey().indexOf("_"));
                logger.debug("#### Checking | prefixForColName :: {} ", prefixForColName);
                if ( prefixForColName.equalsIgnoreCase(triggerRequest.getBaseTableName()) ) {
                    isSame = true;
                    break;
                }

            }
        }

        logger.debug("#### checkForAggregateTableIsSameAsDataJsonTable :: " + isSame);
        return isSame;
    }
}
