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

import javax.management.Query;
import java.net.SocketException;
import java.util.*;

/**
 * Created by shazra on 8/14/15.
 */

class InnerJoinEligibilityCheck {
    private List<String> columnNames;
    private List<Object> objects;
    boolean insertToInnerJoinEligible = true;

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<Object> getObjects() {
        return objects;
    }

    public void setObjects(List<Object> objects) {
        this.objects = objects;
    }

    public boolean isInsertToInnerJoinEligible() {
        return insertToInnerJoinEligible;
    }

    public void setInsertToInnerJoinEligible(boolean insertToInnerJoinEligible) {
        this.insertToInnerJoinEligible = insertToInnerJoinEligible;
    }
}

public class InnerJoinOperation extends GenericOperation {
    private static final Logger logger = LoggerFactory.getLogger(InnerJoinOperation.class);
    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Table viewConfig;

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
    }

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public List<Table> getInputViewTable() {
        return inputViewTables;
    }

    public void setInputViewTable(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public List<Table> getOperationViewTable() {
        return operationViewTables;
    }

    public void setOperationViewTable(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public static InnerJoinOperation getInstance(List<Table> inputViewTable,
                                                 List<Table> operationViewTable) {
        InnerJoinOperation innerJoinOperation = new InnerJoinOperation();
        innerJoinOperation.setInputViewTable(inputViewTable);
        innerJoinOperation.setOperationViewTable(operationViewTable);
        return innerJoinOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        Statement reverseJoinRecordFetchQuery = null;
        logger.debug("##### Entering insert trigger for InnerJoin Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        Table reverseJoinTable = inputViewTables.get(0);
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        PrimaryKey innerJoinViewPrimaryKey = null;
        List<String> baseTablesInvolved = viewConfig.getRefBaseTables();
        boolean viewMaintenanceEligibility = false;
        for ( String baseTableName : baseTablesInvolved ) {
            if ( baseTableName.equalsIgnoreCase(triggerRequest.getBaseTableKeySpace()
                    + "." + triggerRequest.getBaseTableName()) ) {
                viewMaintenanceEligibility = true;
            }
        }
        if ( !viewMaintenanceEligibility ) {
            logger.debug("### The table {} does not belong to the inner join view.", triggerRequest.getBaseTableKeySpace()
                    + "." + triggerRequest.getBaseTableName());
            return true;
        } else {
            logger.debug("### Inner join view maintenance starts for {}", triggerRequest.getBaseTableKeySpace() + "." +
                    triggerRequest.getBaseTableName());
        }

        // TODO:: Use the method ViewMaintenanceUtilities.getJoinTablePrimaryKey rather than writing again here

        Map<String, ColumnDefinition> reverseJoinTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                reverseJoinTable.getKeySpace(), reverseJoinTable.getName());
        for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : reverseJoinTableDesc.entrySet() ) {
            if ( columnDefinitionEntry.getValue().isPartitionKey() ) {
                innerJoinViewPrimaryKey = new PrimaryKey(columnDefinitionEntry.getKey(), columnDefinitionEntry.getValue()
                        .type.toString(), "");

                while ( dataIter.hasNext() ) {
                    String tempDataKey = (String) dataIter.next();
                    logger.debug("Key: " + tempDataKey);
                    logger.debug("Value: " + dataJson.get(tempDataKey));

                    if ( innerJoinViewPrimaryKey.getColumnName().equalsIgnoreCase(tempDataKey) ) {
                        if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                            innerJoinViewPrimaryKey.setColumnValueInString((String) dataJson.get(tempDataKey));
                        } else if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                            innerJoinViewPrimaryKey.setColumnValueInString(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        }
                        break;
                    }
                }
            }

        }


        logger.debug("### Inner join primary key = " + innerJoinViewPrimaryKey);

        if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            reverseJoinRecordFetchQuery = QueryBuilder.select().all().from(reverseJoinTable.getKeySpace(),
                    reverseJoinTable.getName()).where(QueryBuilder.eq(innerJoinViewPrimaryKey.getColumnName(),
                    Integer.parseInt(innerJoinViewPrimaryKey.getColumnValueInString())));
        } else if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            reverseJoinRecordFetchQuery = QueryBuilder.select().all().from(reverseJoinTable.getKeySpace(),
                    reverseJoinTable.getName()).where(QueryBuilder.eq(innerJoinViewPrimaryKey.getColumnName(),
                    innerJoinViewPrimaryKey));
        }


        logger.debug("#### reverseJoinRecordFetchQuery  || " + reverseJoinRecordFetchQuery);

        List<Row> existingReverseJoinRecords = null;
        try {
            existingReverseJoinRecords = CassandraClientUtilities.commandExecution(
                    CassandraClientUtilities.getEth0Ip(), reverseJoinRecordFetchQuery);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }
        boolean insertToInnerJoinEligible = true;
        List<String> columnNamesCurrentJK = null;
        List<Object> objectsCurrentJK = null;
        //TODO: Remove all the null checks and use is NUll
        //TODO: If a column is null then break else check for type and get the values which will be inserted to innerjoin view table

        List<String> joinKeyData = new ArrayList<>();
        joinKeyData.add(innerJoinViewPrimaryKey.getColumnName());
        joinKeyData.add(innerJoinViewPrimaryKey.getColumnInternalCassType());


        InnerJoinEligibilityCheck innerJoinEligibilityCheck = checkAndAssignObjectsInnerJoinQuery(existingReverseJoinRecords);
        logger.debug("#### insertToInnerJoinEligible :: " + innerJoinEligibilityCheck.isInsertToInnerJoinEligible());


        logger.debug("#### columnnames:: " + innerJoinEligibilityCheck.getColumnNames());
        logger.debug("#### objects :: " + innerJoinEligibilityCheck.getObjects());

        insertToInnerJoinEligible = innerJoinEligibilityCheck.isInsertToInnerJoinEligible();


        String statusCurJoinKey = ViewMaintenanceUtilities.checkForChangeInJoinKeyInDeltaView(joinKeyData, deltaTableRecord);

        if ( !statusCurJoinKey.equalsIgnoreCase("new") ) {
            PrimaryKey oldInnerJoinPrimaryKey = ViewMaintenanceUtilities.createOldJoinKeyfromNewValue(innerJoinViewPrimaryKey,
                    deltaTableRecord);

            List<Row> oldReverseJoinRecords = getExistingRecordIfExistsInReverseJoinViewTable(oldInnerJoinPrimaryKey,
                    reverseJoinTable);

            logger.debug("#### Old reverse join records :: " + oldReverseJoinRecords);

            InnerJoinEligibilityCheck innerJoinEligibilityCheckOldJoinKey = checkAndAssignObjectsInnerJoinQuery(oldReverseJoinRecords);

            if ( innerJoinEligibilityCheckOldJoinKey.isInsertToInnerJoinEligible() ) {

                logger.debug("#### Old join key satisfied inner join rules!!!");

                List<String> columnNamesOldJoinKey = innerJoinEligibilityCheckOldJoinKey.getColumnNames();

                List<Object> objectsOldJoinKey = innerJoinEligibilityCheckOldJoinKey.getObjects();

                Statement insertIntoInnerJoinOldJoinKeyDataQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                        operationViewTables.get(0).getName()).values(columnNamesOldJoinKey.toArray(new String
                        [columnNamesOldJoinKey.size()]), objectsOldJoinKey.toArray());

                try {
                    CassandraClientUtilities.commandExecution(
                            CassandraClientUtilities.getEth0Ip(), insertIntoInnerJoinOldJoinKeyDataQuery);
                } catch ( SocketException e ) {
                    logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
                }
            } else {
                logger.debug("### Old join key does not satisfy the inner join rules now!!!");

                Row existingInnerJoinOldRecord = getExistingRecordFromInnerJoinTable(oldInnerJoinPrimaryKey);

                if ( existingInnerJoinOldRecord != null ) {

                    // Store the row which is about to get deleted in the cache table

                    Table cacheTable = new Table();

                    cacheTable.setName(operationViewTables.get(0).getName().replaceAll("inner", "innercache"));

                    cacheTable.setKeySpace(operationViewTables.get(0).getKeySpace());

                    cacheTable.setColumns(operationViewTables.get(0).getColumns());

                    logger.debug("#### Cache table config :: " + cacheTable);

                    try {
                        ViewMaintenanceUtilities.storeJoinRowInCache(existingInnerJoinOldRecord, cacheTable);
                    } catch ( SocketException e ) {
                        logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
                    }

                    logger.debug("#### Old join key needs to be deleted as it no longer satisfies inner join rules");

                    deleteInnerJoinTable(oldInnerJoinPrimaryKey);
                }
            }

        }

        logger.debug("#### case:: For the current entry to the base table!!");
        if ( insertToInnerJoinEligible ) {
            columnNamesCurrentJK = innerJoinEligibilityCheck.getColumnNames();
            objectsCurrentJK = innerJoinEligibilityCheck.getObjects();
            Statement insertQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).values(columnNamesCurrentJK
                    .toArray(new String[columnNamesCurrentJK.size()]), objectsCurrentJK.toArray());

            logger.debug("#### Insert query in innerjoin view table :: " + insertQuery);

            try {
                CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), insertQuery);
            } catch ( SocketException e ) {
                logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
            }

        } else {
            Row existingRecordInnerJoinTable = getExistingRecordFromInnerJoinTable(innerJoinViewPrimaryKey);
            logger.debug("#### existingRecordInnerJoinTable :: " + existingRecordInnerJoinTable);

            if ( existingRecordInnerJoinTable != null ) {
                logger.debug("#### deleting the existing row as it does not satisfy inner join rules");
                deleteInnerJoinTable(innerJoinViewPrimaryKey);
            }

        }
        return true;
    }

    /**
     * Returns the row with existing record if there exists else returns null
     **/
    private List<Row> getExistingRecordIfExistsInReverseJoinViewTable(PrimaryKey reverseJoinViewTablePrimaryKey, Table
            reverseJoinTableConfig) {
        Statement existingRecordQuery = null;
        // Checking if there is an already existing entry for the join key received
        if ( reverseJoinViewTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

            existingRecordQuery = QueryBuilder.select().all().from(reverseJoinTableConfig.getKeySpace(),
                    reverseJoinTableConfig.getName()).where(QueryBuilder
                    .eq(reverseJoinViewTablePrimaryKey.getColumnName(),
                            Integer.parseInt(reverseJoinViewTablePrimaryKey.getColumnValueInString())));
        } else if ( reverseJoinViewTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            existingRecordQuery = QueryBuilder.select().all().from(reverseJoinTableConfig.getKeySpace(),
                    reverseJoinTableConfig.getName()).where(QueryBuilder
                    .eq(reverseJoinViewTablePrimaryKey.getColumnName(),
                            reverseJoinViewTablePrimaryKey.getColumnValueInString()));
        }

        logger.debug("#### Existing Record Query :: " + existingRecordQuery);

        List<Row> existingRows = null;
        try {
            existingRows = CassandraClientUtilities.commandExecution(
                    CassandraClientUtilities.getEth0Ip(), existingRecordQuery);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }


        if ( existingRows.size() > 0 ) {
            logger.debug("#### Existing record in reverse join view table :: " + existingRows.get(0));
            return existingRows;
        }

        return null;
    }


    private InnerJoinEligibilityCheck checkAndAssignObjectsInnerJoinQuery(List<Row> existingReverseJoinRecords) {
        InnerJoinEligibilityCheck innerJoinEligibilityCheck = new InnerJoinEligibilityCheck();

        if ( existingReverseJoinRecords != null && existingReverseJoinRecords.size() > 0 ) {
            List<String> columnNames = new ArrayList<>();
            List<Object> objects = new ArrayList<>();
            Row existingReverseJoinRecord = existingReverseJoinRecords.get(0);
            for ( Column column : operationViewTables.get(0).getColumns() ) {
                logger.debug("#### Checking | executing column :: " + column);
                if ( column.isPrimaryKey() ) {
                    String javaDataTypePK = ViewMaintenanceUtilities.getJavaDataTypeFromCQL3DataType(column.getDataType());
                    if ( javaDataTypePK.equalsIgnoreCase("Integer") ) {
                        if ( existingReverseJoinRecord.getInt(column.getName()) == 0 ) {
                            innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getInt(column.getName()));
                        }
                    } else if ( javaDataTypePK.equalsIgnoreCase("String") ) {
                        if ( existingReverseJoinRecord.isNull(column.getName()) ) {
                            innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getString(column.getName()));
                        }
                    }
                } else if ( column.getDataType().equalsIgnoreCase("list<int>") ||
                        column.getDataType().equalsIgnoreCase("list <int>") ) {
                    // For the actual primary key of datatype int
                    List<Integer> actualPKListInReverseJoinView = existingReverseJoinRecord.getList(column.getName(),
                            Integer.class);
                    logger.debug("#### Checking : actualPrimaryKeyCol(list) in ReverseJoin Table : "
                            + actualPKListInReverseJoinView);
                    if ( actualPKListInReverseJoinView == null || actualPKListInReverseJoinView.isEmpty() ) {
                        innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
                        break;
                    } else {
                        columnNames.add(column.getName());
                        objects.add(existingReverseJoinRecord.getList(column.getName(), Integer.class));
                    }

                } else if ( column.getDataType().equalsIgnoreCase("list<text>") ||
                        column.getDataType().equalsIgnoreCase("list <text>") ) {
                    // For the actual primary key of datatype String
                    List<String> actualPKListInReverseJoinView = existingReverseJoinRecord.getList(column.getName(), String.class);
                    logger.debug("#### Checking : actualPrimaryKeyCol(list) in ReverseJoin Table : " + actualPKListInReverseJoinView);
                    if ( actualPKListInReverseJoinView == null || actualPKListInReverseJoinView.isEmpty() ) {
                        innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
                        break;
                    } else {
                        columnNames.add(column.getName());
                        objects.add(existingReverseJoinRecord.getList(column.getName(), String.class));
                    }
                } else {
                    if ( column.getDataType().equalsIgnoreCase("map <int,text>") ||
                            column.getDataType().equalsIgnoreCase("map <int, text>") ) {
                        Map<Integer, String> reverseJoinMap = existingReverseJoinRecord.getMap(column.getName(),
                                Integer.class, String.class);
                        if ( reverseJoinMap == null || reverseJoinMap.isEmpty() ) {
                            innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getMap(column.getName(), Integer.class, String.class));
                        }
                    } else if ( column.getDataType().equalsIgnoreCase("map <int,int>") ||
                            column.getDataType().equalsIgnoreCase("map <int, int>") ) {
                        Map<Integer, Integer> reverseJoinMap = existingReverseJoinRecord.getMap(column.getName(),
                                Integer.class, Integer.class);
//                        logger.debug("#### Checking : reverseJoinMap : " + reverseJoinMap);
                        if ( reverseJoinMap == null || reverseJoinMap.isEmpty() ) {
                            innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getMap(column.getName(), Integer.class, Integer.class));
                        }
                    }
                }
            }

            innerJoinEligibilityCheck.setColumnNames(columnNames);
            innerJoinEligibilityCheck.setObjects(objects);
        } else {
            innerJoinEligibilityCheck.setInsertToInnerJoinEligible(false);
        }


        return innerJoinEligibilityCheck;
    }

    private void deleteInnerJoinTable(PrimaryKey innerJoinPrimaryKey) {
        Statement deleteQuery = null;

        if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            deleteQuery = QueryBuilder.delete().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(innerJoinPrimaryKey.getColumnName(),
                    Integer.parseInt(innerJoinPrimaryKey.getColumnValueInString())));
        } else if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            deleteQuery = QueryBuilder.delete().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(innerJoinPrimaryKey.getColumnName(),
                    innerJoinPrimaryKey.getColumnValueInString()));
        }


        logger.debug("#### Delete query for inner join table :: " + deleteQuery);
        try {
            CassandraClientUtilities.deleteCommandExecution(CassandraClientUtilities.getEth0Ip(), deleteQuery);
        } catch ( SocketException e ) {
            logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }
    }


    private Row getExistingRecordFromInnerJoinTable(PrimaryKey innerJoinPrimaryKey) {
        Row existingRecordInnerJoinTable = null;

        Statement existingInnerJoinRecordQuery = null;

        if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

            existingInnerJoinRecordQuery = QueryBuilder.select().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(innerJoinPrimaryKey.getColumnName(),
                    Integer.parseInt(innerJoinPrimaryKey.getColumnValueInString())));
        } else if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            existingInnerJoinRecordQuery = QueryBuilder.select().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder.eq(innerJoinPrimaryKey.getColumnName(),
                    innerJoinPrimaryKey.getColumnValueInString()));
        }
        List<Row> existingRecords = null;
        try {
            existingRecords = CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(),
                    existingInnerJoinRecordQuery);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }
        if ( existingRecords != null && existingRecords.size() > 0 ) {

            existingRecordInnerJoinTable = existingRecords.get(0);
        }

        logger.debug("#### Existing record in inner join table:: " + existingRecordInnerJoinTable);

        return existingRecordInnerJoinTable;
    }

    @Override
    public boolean updateTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public boolean deleteTrigger(TriggerRequest triggerRequest) {

        logger.debug("##### Entering delete trigger for InnerJoin Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure :: {}", this.inputViewTables);

        PrimaryKey innerJoinPrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableConfigWithoutValue(operationViewTables
                .get(0).getKeySpace(), operationViewTables.get(0).getName());

        if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            innerJoinPrimaryKey.setColumnValueInString(deltaTableRecord.getInt(innerJoinPrimaryKey.getColumnName() +
                    DeltaViewTrigger.CURRENT) + "");
        } else if ( innerJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            innerJoinPrimaryKey.setColumnValueInString(deltaTableRecord.getString(innerJoinPrimaryKey.getColumnName() +
                    DeltaViewTrigger.CURRENT));
        }

        logger.debug("##### Inner join primary key :: " + innerJoinPrimaryKey);

        List<Row> existingRowReverseJoinTable = getExistingRecordIfExistsInReverseJoinViewTable(innerJoinPrimaryKey,
                inputViewTables.get(0));

        logger.debug("#### Existing record from reverse join table :: " + existingRowReverseJoinTable);


        if ( existingRowReverseJoinTable != null ) {

            InnerJoinEligibilityCheck innerJoinEligibilityCheck = checkAndAssignObjectsInnerJoinQuery(existingRowReverseJoinTable);
            if ( innerJoinEligibilityCheck.isInsertToInnerJoinEligible() ) {
                List<String> columnNamesOldJoinKey = innerJoinEligibilityCheck.getColumnNames();

                List<Object> objectsOldJoinKey = innerJoinEligibilityCheck.getObjects();

                Statement insertIntoInnerJoinOldJoinKeyDataQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                        operationViewTables.get(0).getName()).values(columnNamesOldJoinKey.toArray(new String
                        [columnNamesOldJoinKey.size()]), objectsOldJoinKey.toArray());

                logger.debug("##### insertIntoInnerJoinOldJoinKeyDataQuery :: " + insertIntoInnerJoinOldJoinKeyDataQuery);

                try {
                    CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), insertIntoInnerJoinOldJoinKeyDataQuery);
                } catch ( SocketException e ) {
                    logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
                }
            } else {
                Row existingRowInnerJoinTable = null;
                try {
                    existingRowInnerJoinTable = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKey,
                            operationViewTables.get(0));
                } catch ( SocketException e ) {
                    logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
                }

                logger.debug("#### Existing inner join record which does not qualify to be in inner join :: " + existingRowInnerJoinTable);

                if ( existingRowInnerJoinTable != null ) {
                    Table cacheTable = new Table();

                    cacheTable.setName(operationViewTables.get(0).getName().replaceAll("inner", "innercache"));

                    cacheTable.setKeySpace(operationViewTables.get(0).getKeySpace());

                    cacheTable.setColumns(operationViewTables.get(0).getColumns());

                    logger.debug("#### Cache table config :: " + cacheTable);

                    try {
                        ViewMaintenanceUtilities.storeJoinRowInCache(existingRowInnerJoinTable, cacheTable);
                    } catch ( SocketException e ) {
                        logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
                    }

                    deleteInnerJoinTable(innerJoinPrimaryKey);
                }
            }

        } else {
            Row existingRowInnerJoinTable = null;
            try {
                existingRowInnerJoinTable = ViewMaintenanceUtilities.getExistingRecordIfExists(innerJoinPrimaryKey,
                        operationViewTables.get(0));
            } catch ( SocketException e ) {
                logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
            }

            logger.debug("#### Existing inner join record which is not there in Reverse join :: " + existingRowInnerJoinTable);

            if ( existingRowInnerJoinTable != null ) {
                Table cacheTable = new Table();

                cacheTable.setName(operationViewTables.get(0).getName().replaceAll("inner", "innercache"));

                cacheTable.setKeySpace(operationViewTables.get(0).getKeySpace());

                cacheTable.setColumns(operationViewTables.get(0).getColumns());

                logger.debug("#### Cache table config :: " + cacheTable);

                try {
                    ViewMaintenanceUtilities.storeJoinRowInCache(existingRowInnerJoinTable, cacheTable);
                } catch ( SocketException e ) {
                    logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
                }
                deleteInnerJoinTable(innerJoinPrimaryKey);
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "InnerJoinOperation{" +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }

}
