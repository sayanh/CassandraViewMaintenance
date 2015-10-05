package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.config.WhereUtilityProcessor;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.*;

/**
 * Created by shazra on 8/14/15.
 */
public class WhereOperation extends GenericOperation {
    private static final Logger logger = LoggerFactory.getLogger(WhereOperation.class);
    private Row deltaTableRecord;
    private Expression whereExpression;
    private Table viewConfig;
    private String TABLE_PREFIX = "";

    public void setWhereExpression(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    //    private List<Table> inputViewTable;
    private List<Table> operationViewTables;


    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
        TABLE_PREFIX = viewConfig.getName() + "_where_";
    }

    public void setOperationViewTable(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

//    public void setDeltaTableRecord(Row deltaTableRecord) {
//        this.deltaTableRecord = deltaTableRecord;
//    }

    public static WhereOperation getInstance(List<Table> inputViewTable,
                                             List<Table> operationViewTable) {
        WhereOperation whereOperation = new WhereOperation();
//        whereOperation.setInputViewTable(inputViewTable);
        whereOperation.setOperationViewTable(operationViewTable);
        return whereOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering insert trigger for Where Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Trigger request :: " + triggerRequest);
        Statement fetchExistingRow = null;
//        boolean isResultSuccessful = false;
        List<Expression> whereExpressions = ViewMaintenanceUtilities.parseWhereExpression(whereExpression);
        /**
         * Applicable for one kind of operation i.e. either AND or OR between
         * any number of expressions
         * Future prospects: Can be generalized to variety of combinations
         **/

        Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                triggerRequest.getBaseTableKeySpace(), triggerRequest.getBaseTableName());
        logger.debug("### Checking -- table description for baseTable:{} is \n {}", triggerRequest.getBaseTableKeySpace()
                + "." + triggerRequest.getBaseTableName(), baseTableDesc);

//        boolean deciderForNewRow = false; // If true then the stream of information needs to be stored else deleted if already present

        Map<String, List<String>> columnMap = null; // Format: <Column_name> , <internalcassandra type, value, isPrimaryKey>
        String viewTableName = TABLE_PREFIX + triggerRequest.getBaseTableName();
        Table viewTableConfig = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
                triggerRequest, operationViewTables);
        logger.debug("### Checking --- viewTableName  :: " + viewTableName);
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        WhereUtilityProcessor whereUtilityProcessor = getColumnMapWhereTableFromTriggerRequest(triggerRequest,
                baseTableDesc, viewConfig, viewTableConfig);

        fetchExistingRow = whereUtilityProcessor.getFetchExistingRecordQuery();
        columnMap = whereUtilityProcessor.getColumnMap();
        String primaryKey = whereUtilityProcessor.getWherePrimaryKey().getColumnName();


        logger.debug("### Stream available for maintenance :: " + columnMap);


        for ( Table whereTable : operationViewTables ) {

            String targetWhereTable = whereTable.getName();
            String targetWhereTableArr[] = targetWhereTable.split("_");
            String targetTableDerivedFromOperationTable = targetWhereTableArr[2];
            if ( targetTableDerivedFromOperationTable.equalsIgnoreCase(triggerRequest.getBaseTableName()) ) {
                logger.debug("#### Checking --- Target where table is {}", whereTable.getKeySpace()
                        + "." + whereTable.getName());
                boolean updateEligibleIndicator = false;
                boolean insertEligibleIndicator = false;
                if ( whereExpression instanceof AndExpression ) {
                    for ( Expression expression : whereExpressions ) {
                        Column column = ViewMaintenanceUtilities.getColumnObject(expression);
                        logger.debug("### Evaluating expression :: " + expression.toString());
                        if ( column.getTable().getName().equalsIgnoreCase(targetTableDerivedFromOperationTable) ) {
                            logger.debug("### Concerned expression :: " + expression.toString() + " for base table :: "
                                    + targetTableDerivedFromOperationTable);
                            logger.debug("### Fetch Existing rows query ::" + fetchExistingRow);
                            // Fetching existing record from the view table if exists
                            List<Row> existingRecords = null;
                            try {
                                existingRecords = CassandraClientUtilities.commandExecution(
                                        CassandraClientUtilities.getEth0Ip(),
                                        fetchExistingRow);
                            } catch ( SocketException e ) {
                                logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
                            }
                            if ( existingRecords.size() > 0 ) {
                                Row existingRecord = existingRecords.get(0);
                                logger.debug("### Existing record ### " + existingRecord);

                                if ( ViewMaintenanceUtilities.checkExpression(expression, columnMap) ) {
                                    updateEligibleIndicator = true;
                                } else {
                                    PrimaryKey whereTablePK = new PrimaryKey(primaryKey, columnMap.get(primaryKey).get(0),
                                            columnMap.get(primaryKey).get(1));
                                    deleteFromWhereViewTable(whereTablePK, whereTable);
                                    updateEligibleIndicator = false;
                                    break;
                                }
                            } else {
                                // Insert - This is a new entry
                                logger.debug("### New Entry for whereViewTable ###");
                                if ( ViewMaintenanceUtilities.checkExpression(expression, columnMap) ) {

                                    insertEligibleIndicator = true;
                                } else {
                                    insertEligibleIndicator = false;
                                    break;
                                }
                            }
                        }
                    }

                    if ( updateEligibleIndicator ) {
                        // Update if exists
                        updateIntoWhereViewTable(columnMap, whereTable);
                    } else if ( insertEligibleIndicator ) {
                        insertIntoWhereViewTable(columnMap, whereTable);
                    }
                } else if ( whereExpression instanceof OrExpression ) {
                    for ( Expression expression : whereExpressions ) {
                        Column column = ViewMaintenanceUtilities.getColumnObject(expression);
                    }
                } else if ( whereExpressions.size() == 1 ) {
                    logger.debug("### Evaluating expression :: " + whereExpression.toString());
                    logger.debug("### Fetch Existing rows query ::" + fetchExistingRow);
                    Column column = ViewMaintenanceUtilities.getColumnObject(whereExpression);
                    if ( column.getTable().getName().equalsIgnoreCase(targetTableDerivedFromOperationTable) ) {

                        List<Row> existingRecords = null;
                        try {
                            existingRecords = CassandraClientUtilities.commandExecution(
                                    CassandraClientUtilities.getEth0Ip(),
                                    fetchExistingRow);
                        } catch ( SocketException e ) {
                            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
                        }
                        if ( existingRecords != null && existingRecords.size() > 0 ) {
                            Row existingRecord = existingRecords.get(0);
                            logger.debug("### Existing record ### " + existingRecord);

                            if ( ViewMaintenanceUtilities.checkExpression(whereExpression, columnMap) ) {
                                // Update if exists
                                updateIntoWhereViewTable(columnMap, whereTable);
                            } else {
                                logger.debug("### New record does not comply with the view rules..hence deleted###");
                                logger.debug("### Primary Key ###" + primaryKey);
                                PrimaryKey whereTablePK = new PrimaryKey(primaryKey, columnMap.get(primaryKey).get(0),
                                        columnMap.get(primaryKey).get(1));
                                deleteFromWhereViewTable(whereTablePK, whereTable);
                            }
                        } else {
                            // Insert - This is a new entry
                            logger.debug("### New Entry for whereViewTable ###" + columnMap);
                            if ( ViewMaintenanceUtilities.checkExpression(whereExpression, columnMap) ) {
                                insertIntoWhereViewTable(columnMap, whereTable);
                            }
                        }
                    }
                }

                break;
            }

        }

        return true;

    }


    private void insertIntoWhereViewTable(Map<String, List<String>> columnMap, Table whereTable) {

        logger.debug("### Checking --- columnMap " + columnMap);
        logger.debug("### Checking --- whereTable " + whereTable);
        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();

        for ( Map.Entry<String, List<String>> column : columnMap.entrySet() ) {
            columnNames.add(column.getKey());
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                    .equalsIgnoreCase("Integer") ) {

                objects.add(Integer.parseInt(column.getValue().get(1)));

            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                    .equalsIgnoreCase("String") ) {

                objects.add(column.getValue().get(1));
            }
        }

        Statement insertQuery = QueryBuilder.insertInto(whereTable.getKeySpace(), whereTable.getName())
                .values(columnNames.toArray(new String[columnNames.size()]), objects.toArray());

        logger.debug("### Final insert query to where view table:: " + insertQuery);

        try {
            CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), insertQuery);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }
    }

    private void updateIntoWhereViewTable(Map<String, List<String>> columnMap, Table whereTable) {

        StringBuffer updateQuery = new StringBuffer("Update " + whereTable.getKeySpace() + "." +
                whereTable.getName() + " set ");
        String whereStr = "";
        for ( Map.Entry<String, List<String>> column : columnMap.entrySet() ) {
            if ( column.getValue().get(2).equalsIgnoreCase("true") ) {
                // Setting the where string for primary key
                if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                        .equalsIgnoreCase("Integer") ) {
                    whereStr = " where " + column.getKey() + " = " + column.getValue().get(1);
                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                        .equalsIgnoreCase("String") ) {
                    whereStr = " where " + column.getKey() + " = '" + column.getValue().get(1) + "'";
                }
            } else {
                // For all other columns
                if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                        .equalsIgnoreCase("Integer") ) {
                    updateQuery.append(column.getKey() + " = " + column.getValue().get(1) + ", ");
                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                        .equalsIgnoreCase("String") ) {
                    updateQuery.append(column.getKey() + " = '" + column.getValue().get(1) + "'" + ", ");
                }
            }


        }


        updateQuery = ViewMaintenanceUtilities.removesCommaSpace(updateQuery);
        logger.debug("### Checking :: update without where clause :: " + updateQuery.toString());
        logger.debug("### Checking :: where clause :: " + whereStr);
        updateQuery.append(whereStr);
        logger.debug("### Final update query to where view table :: " + updateQuery.toString());

        try {
            CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), updateQuery.toString());
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }


    }

    private void deleteFromWhereViewTable(PrimaryKey whereTablePK, Table whereTable) {
        logger.debug("### Checking -- primary key reached here {}", whereTablePK);
        Statement statement = null;
        if ( whereTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            statement = QueryBuilder.delete().from(whereTable.getKeySpace(),
                    whereTable.getName()).where(QueryBuilder.eq(whereTablePK.getColumnName(),
                    Integer.parseInt(whereTablePK.getColumnValueInString())));
        } else if ( whereTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            statement = QueryBuilder.delete().from(whereTable.getKeySpace(),
                    whereTable.getName()).where(QueryBuilder.eq(whereTablePK.getColumnName(),
                    whereTablePK.getColumnValueInString()));
        }


        logger.debug("### Final delete query for where view table :: " + statement.toString());

        try {
            CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), statement);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }


    }


    @Override
    public boolean updateTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public boolean deleteTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering delete trigger for Where Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getDeletedRowDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Trigger request :: " + triggerRequest);

        Table whereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList
                (triggerRequest, operationViewTables);
        Map<String, ColumnDefinition> whereTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                triggerRequest.getBaseTableKeySpace(), triggerRequest.getBaseTableName());
        PrimaryKey deletePrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableDescWithoutValue(whereTableDesc);

        String[] whereStringArr = triggerRequest.getWhereString().split("=");
        if ( deletePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            deletePrimaryKey.setColumnValueInString(whereStringArr[1].trim());
        } else if ( deletePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {

            deletePrimaryKey.setColumnValueInString(whereStringArr[1].trim().replace("'", ""));
        }


        Row recordTobeDeleted = null;
        try {
            recordTobeDeleted = ViewMaintenanceUtilities.getExistingRecordIfExists(deletePrimaryKey, whereTable);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }

        logger.debug("#### Record to be deleted :: " + recordTobeDeleted);

        if (recordTobeDeleted != null) {
            logger.debug("##### Deleting the record for key :: " + deletePrimaryKey);
            deleteFromWhereViewTable(deletePrimaryKey, whereTable);
        }

        return false;
    }

    @Override
    public String toString() {
        return "WhereOperation{" +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }

    private static WhereUtilityProcessor getColumnMapWhereTableFromTriggerRequest(
            TriggerRequest triggerRequest, Map<String, ColumnDefinition> baseTableDesc,
            Table viewConfig, Table whereTable) {
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        Map<String, List<String>> columnMap = new HashMap<>();
        PrimaryKey whereTablePrimaryKey = null;

        String viewTableName = whereTable.getName();

        WhereUtilityProcessor whereUtilityProcessor;
        Statement fetchExistingRowQuery = null;

        while ( dataIter.hasNext() ) {
            String tempDataKey = (String) dataIter.next();
//            logger.debug("Key: " + tempDataKey);
//            logger.debug("Value: " + dataJson.get(tempDataKey));
            for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : baseTableDesc.entrySet() ) {
                ColumnDefinition columnDefinition = columnDefinitionEntry.getValue();
                if ( tempDataKey.equalsIgnoreCase(columnDefinition.name.toString()) ) {
                    List<String> tempList = new ArrayList<>(); // Format of storing: internalcassandra type, value, isPrimaryKey

                    tempList.add(columnDefinition.type.toString());

                    if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                            columnDefinition.type.toString()).equalsIgnoreCase("String") ) {
                        tempList.add(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        if ( columnDefinition.isPartitionKey() ) {
                            fetchExistingRowQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewTableName)
                                    .where(QueryBuilder.eq(columnDefinition.name.toString(),
                                            ((String) dataJson.get(tempDataKey)).replaceAll("'", "")));
                        }
                    } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                            columnDefinition.type.toString()).equalsIgnoreCase("Integer") ) {
                        tempList.add((String) dataJson.get(tempDataKey));
                        if ( columnDefinition.isPartitionKey() ) {
                            fetchExistingRowQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewTableName)
                                    .where(QueryBuilder.eq(columnDefinition.name.toString(),
                                            Integer.parseInt(((String) dataJson.get(tempDataKey)))));
                        }
                    }
                    tempList.add(columnDefinition.isPartitionKey() ? "true" : "false");

                    columnMap.put(tempDataKey, tempList);
                    if ( columnDefinition.isPartitionKey() ) {
                        whereTablePrimaryKey = new PrimaryKey(tempDataKey, columnDefinitionEntry
                                .getValue().type.toString(),
                                ((String) dataJson.get(tempDataKey)).replaceAll("'", ""));

                    }
                    break;
                }
            }

        }

        whereUtilityProcessor = new WhereUtilityProcessor();

        whereUtilityProcessor.setColumnMap(columnMap);
        whereUtilityProcessor.setWherePrimaryKey(whereTablePrimaryKey);
        whereUtilityProcessor.setFetchExistingRecordQuery(fetchExistingRowQuery);

        return whereUtilityProcessor;
    }
}
