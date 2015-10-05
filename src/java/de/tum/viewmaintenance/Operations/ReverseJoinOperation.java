package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.tools.NodeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.*;

/**
 * Created by shazra on 8/15/15.
 */

public class ReverseJoinOperation extends GenericOperation {
    private static final Logger logger = LoggerFactory.getLogger(ReverseJoinOperation.class);
    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Table viewConfig;
    private Expression whereExpression;
    private List<Join> joins;
    final static String WHERE_TABLE_INDENTIFIER = "_where_";

    public void setWhereExpression(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    public void setJoins(List<Join> joins) {
        this.joins = joins;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
    }

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

//    public void setDeltaTableRecord(Row deltaTableRecord) {
//        this.deltaTableRecord = deltaTableRecord;
//    }

    public GenericOperation getSqlOperation() {
        return sqlOperation;
    }

    public void setSqlOperation(GenericOperation sqlOperation) {
        this.sqlOperation = sqlOperation;
    }

    public List<Table> getInputViewTable() {
        return inputViewTables;
    }

    public void setInputViewTable(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public List<Table> getOperationViewTables() {
        return operationViewTables;
    }

    public void setOperationViewTables(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering insert trigger for ReverseJoin Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure :: {}", this.inputViewTables);

        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        EqualsTo onExpresssionJoin = (EqualsTo) joins.get(0).getOnExpression();
        List<String> tablesInvolved = viewConfig.getRefBaseTables();
        String primaryColName = ((Column) onExpresssionJoin.getLeftExpression()).getColumnName(); //Primary col of the reverse view table
        Map<String, Map<String, ColumnDefinition>> tableDesc = new HashMap<>();
        String primaryValue = "";
        PrimaryKey actualPrimaryKey = new PrimaryKey();
        PrimaryKey reverseJoinViewTablePrimaryKey = null;
        Table whereTableInvolved = null;

        List<Expression> whereExpressions = ViewMaintenanceUtilities.getParticularWhereExpressionBasedOnBaseTableOperation(viewConfig.getSqlString(),
                triggerRequest.getBaseTableName());


        // Getting the concerned whereViewTable involved
        for ( Table whereTable : inputViewTables ) {
            String tempNameArr[] = whereTable.getName().split("_");
            if ( tempNameArr[2].equalsIgnoreCase(triggerRequest.getBaseTableName()) ) {
                whereTableInvolved = whereTable;
                break;
            }
        }

        logger.debug("### Where table involved :: " + whereTableInvolved);


        for ( String tableName : tablesInvolved ) {
            String tempTableNameArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(tableName);
            tableDesc.put(tableName, ViewMaintenanceUtilities.getTableDefinitition(tempTableNameArr[0], tempTableNameArr[1]));
        }

        Map<String, List<String>> columnMap = new HashMap<>();
        // Format::
        // Key: Name of the column
        // Value : CassandraInternalType, ValueInString, isPrimaryKeyForReverseJoinTable

        for ( Map.Entry<String, Map<String, ColumnDefinition>> table : tableDesc.entrySet() ) {

            if ( table.getKey().equalsIgnoreCase(triggerRequest.getBaseTableKeySpace() + "."
                    + triggerRequest.getBaseTableName()) ) { // Running reverseJoin maintenance for the concerned table in the dataJson

                // Finding the actual primary key of the base table
                for ( Map.Entry<String, ColumnDefinition> columnDefinitionMap : table.getValue().entrySet() ) {
                    if ( columnDefinitionMap.getValue().isPartitionKey() ) {
                        actualPrimaryKey.setColumnName(columnDefinitionMap.getKey());
                        actualPrimaryKey.setColumnInternalCassType(columnDefinitionMap.getValue().type.toString());
                        actualPrimaryKey.setColumnJavaType(ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                                columnDefinitionMap.getValue().type.toString()
                        ));
                        break;
                    }
                }


                while ( dataIter.hasNext() ) {
                    List<String> tempColDescList = new ArrayList<>(); // Format: CassandraInternalType, ValueInString, isPrimaryKeyForReverseJoinTable
                    String tempDataKey = (String) dataIter.next();
                    logger.debug("Key: " + tempDataKey);
                    logger.debug("Value: " + dataJson.get(tempDataKey));
                    tempColDescList.add(table.getValue().get(tempDataKey).type.toString()); // Adding Cassandra internal type of the column
                    if ( tempDataKey.equalsIgnoreCase(primaryColName) ) {
                        primaryValue = ((String) dataJson.get(tempDataKey)).replaceAll("'", "");
                        tempColDescList.add(primaryValue);
                        tempColDescList.add("true");
                        columnMap.put(tempDataKey, tempColDescList);
                        reverseJoinViewTablePrimaryKey = new PrimaryKey(primaryColName, table.getValue().get(tempDataKey).type.toString(),
                                primaryValue);
                    } else {
                        tempColDescList.add(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        tempColDescList.add("false");
                        if ( ViewMaintenanceUtilities.checkPresenceOfColumnInDifferentTable(table.getKey(),
                                tempDataKey, tableDesc) ) {
                            columnMap.put(triggerRequest.getBaseTableName() + "_" + tempDataKey, tempColDescList);
                        } else {
                            columnMap.put(tempDataKey, tempColDescList);
                        }

                    }

                    // Setting the value for the actual primary key of the base table in the dataJson
                    if ( actualPrimaryKey.getColumnName().equalsIgnoreCase(tempDataKey) ) {
                        actualPrimaryKey.setColumnValueInString(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                    }
                }

                break;
            }

        }

        logger.debug("#### Values for the columnMap :: " + columnMap);

        Column whereTargetCol = ViewMaintenanceUtilities.getColumnFromExpression(whereExpressions.get(0), columnMap);

        List<String> wherePKData = new ArrayList<>();
        wherePKData.add("");
        wherePKData.add("");
        wherePKData.add(whereTargetCol.getColumnName());

        logger.debug("#### Checking :: wherePKData :: " + wherePKData);

        // Select from the where table to see whether this record which needs to be maintained

        List<Row> whereTableExistingRows = null;
        if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            Statement whereTableExistentQuery = QueryBuilder.select().all().from(whereTableInvolved.getKeySpace(),
                    whereTableInvolved.getName()).where(QueryBuilder.eq(actualPrimaryKey.getColumnName(),
                    Integer.parseInt(actualPrimaryKey.getColumnValueInString())));
            logger.debug("### ViewMaintenance eligibility query(Integer) :: " + whereTableExistentQuery);
            try {
                whereTableExistingRows = CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(),
                        whereTableExistentQuery);
            } catch ( SocketException e ) {
                logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
            }
        } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            Statement whereTableExistentQuery = QueryBuilder.select().all().from(whereTableInvolved.getKeySpace(),
                    whereTableInvolved.getName()).where(QueryBuilder.eq(actualPrimaryKey.getColumnName(),
                    actualPrimaryKey.getColumnValueInString()));
            logger.debug("### ViewMaintenance eligibility query(String) :: " + whereTableExistentQuery);

            try {
                whereTableExistingRows = CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(),
                        whereTableExistentQuery);
            } catch ( SocketException e ) {
                logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
            }
        }
        List<String> joinKeyData = new ArrayList<>();
        joinKeyData.add(primaryColName);
        joinKeyData.add(reverseJoinViewTablePrimaryKey.getColumnInternalCassType());

        logger.debug("#### Checking :: " + joinKeyData);

        // Note: Although we say this is a new join key, it actually means newData(new based on the primary key)
        String statusCurJoinKey = ViewMaintenanceUtilities.checkForChangeInJoinKeyInDeltaView(joinKeyData, deltaTableRecord);

        if ( statusCurJoinKey.equalsIgnoreCase("new") ) {

            logger.debug("#### Case:: new record..need to intelligently update the join key in the join tables #### ");

            if ( whereTableExistingRows.size() == 0 ) {
                logger.debug("#### Case: Newly present data and where does not satisfy...no action taken!!");
                return true;
            } else {
                logger.debug("### whereViewEligibility Results for already existing join key:: " + whereTableExistingRows.get(0));

                logger.debug("### Case(new): When there is corrs value in the where table for the same " +
                        "join key!! ### ");

                logger.debug("Intelligent update of the data for the key!!!");

                intelligentEntryPreAggViewTable(reverseJoinViewTablePrimaryKey, actualPrimaryKey, triggerRequest,
                        columnMap);
            }


        } else {

            logger.debug("#### Case:: {} #### ", statusCurJoinKey);


            Table concernedWhereTable = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(
                    triggerRequest, inputViewTables);

            logger.debug("#### Received concerned where table :: " + concernedWhereTable);


            boolean didOldValueSatisfyWhereClause = ViewMaintenanceUtilities.didOldValueSatisfyWhereClause
                    (viewConfig, triggerRequest, wherePKData, deltaTableRecord, concernedWhereTable);

            logger.debug("Checking ... didOldValueSatisfyWhereClause: " + didOldValueSatisfyWhereClause);

            if ( didOldValueSatisfyWhereClause ) {
                // Delete from the old join key data in the reverse join table
                logger.debug("#### Deleting the contents in the old join key as it does not exist now!!!");
                if ( statusCurJoinKey.equalsIgnoreCase("changed") ) {
                    logger.debug("### Join key changed:: deleting the old one");
                    PrimaryKey oldJoinKey = ViewMaintenanceUtilities.createOldJoinKeyfromNewValue(reverseJoinViewTablePrimaryKey,
                            deltaTableRecord);
                    deleteFromReverseViewTable(oldJoinKey, actualPrimaryKey, triggerRequest, columnMap);
                } else {
                    logger.debug("### Join key unchanged:: deleting the old values");
                    deleteFromReverseViewTable(reverseJoinViewTablePrimaryKey, actualPrimaryKey, triggerRequest, columnMap);
                }
            }


            if ( whereTableExistingRows == null || whereTableExistingRows.size() == 0 ) {
                logger.debug("#### whereTableExistingRows are nil hence nothing to do here!!!");
                // Nothing is required to do
                logger.debug("### The old value was not there and the new value does not satisfy where !! ");

            } else {
                logger.debug("### viewEligibility Results :: " + whereTableExistingRows.get(0));

                logger.debug("### Case(ch/un-changed): When there is some value in the where table for the same " +
                        "join key!! ### ");

                logger.debug("Intelligent update of the data for the key!!!");
                intelligentEntryPreAggViewTable(reverseJoinViewTablePrimaryKey, actualPrimaryKey, triggerRequest,
                        columnMap);

            }
        }
        return false;
    }

    private void deleteFromReverseViewTable(PrimaryKey reverseJoinPrimaryKey, PrimaryKey actualPrimaryKey,
                                            TriggerRequest triggerRequest, Map<String, List<String>> columnMap) {

        logger.debug("#### Checking .. inside deleteFromReverseViewTable ...");
        logger.debug("#### Checking .. reverseJoinPrimaryKey = " + reverseJoinPrimaryKey);
        logger.debug("#### Checking .. actualPrimaryKey = " + actualPrimaryKey);
        Row existingRecord = getExistingRecordIfExistsInReverseJoinViewTable(reverseJoinPrimaryKey);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Clause whereClause = null;

        if ( existingRecord != null ) {
            Map<String, ColumnDefinition> tableDesc = ViewMaintenanceUtilities.getTableDefinitition(operationViewTables
                    .get(0).getKeySpace(), operationViewTables.get(0).getName());

            for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : tableDesc.entrySet() ) {

                logger.debug("#### Checking ... processing columnDefinitionEntry {} #### ", columnDefinitionEntry);

                if ( reverseJoinPrimaryKey.getColumnName().equalsIgnoreCase(columnDefinitionEntry.getKey()) ) {
                    if ( reverseJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                        whereClause = QueryBuilder.eq(reverseJoinPrimaryKey.getColumnName(),
                                Integer.parseInt(reverseJoinPrimaryKey.getColumnValueInString()));
                    } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                        whereClause = QueryBuilder.eq(reverseJoinPrimaryKey.getColumnName(),
                                reverseJoinPrimaryKey.getColumnValueInString());
                    }

                } else if ( columnDefinitionEntry.getKey().equalsIgnoreCase(triggerRequest.getBaseTableName()
                        + "_" + actualPrimaryKey.getColumnName()) || columnDefinitionEntry.getKey()
                        .equalsIgnoreCase(actualPrimaryKey.getColumnName()) ) {
                    String javaDataType = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry
                            .getValue().type.toString());

                    if ( javaDataType.equalsIgnoreCase("list<Integer>") ) {
                        List<Integer> tempList = existingRecord.getList(columnDefinitionEntry.getKey(), Integer.class);
                        List<Integer> newData = new ArrayList<>();
                        for ( int tempActualPK : tempList ) {
                            if ( tempActualPK != Integer.parseInt(actualPrimaryKey.getColumnValueInString()) ) {

                                newData.add(tempActualPK);
                            }
                        }
                        assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));

                    } else if ( javaDataType.equalsIgnoreCase("list<String>") ) {
                        List<String> tempList = existingRecord.getList(columnDefinitionEntry.getKey(), String.class);
                        List<String> newData = new ArrayList<>();
                        for ( String tempActualPK : tempList ) {

                            if ( !tempActualPK.equalsIgnoreCase(actualPrimaryKey.getColumnValueInString()) ) {

                                newData.add(tempActualPK);
                            }
                            newData.add(tempActualPK);
                        }
                        newData.add(actualPrimaryKey.getColumnValueInString());
                        assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));
                    }


                } else {
                    String javaDataType = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue().type
                            .toString());
                    logger.debug("####Checking .... javaDataType :: " + javaDataType);
                    if ( columnMap.containsKey(columnDefinitionEntry.getKey()) ) {
                        if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                            if ( javaDataType.equalsIgnoreCase("Map<Integer, Integer>") ) {
                                Map<Integer, Integer> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                        Integer.class, Integer.class);
                                Map<Integer, Integer> newData = new HashMap<>();
                                for ( Map.Entry<Integer, Integer> tempColEntry : tempCol.entrySet() ) {
                                    if ( tempColEntry.getKey() == Integer.parseInt(actualPrimaryKey.getColumnValueInString()) ) {
                                        logger.debug("### Checking :: Escaped and not added to the result map :: " + tempColEntry);
                                    } else {
                                        newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                    }
                                }
                                assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));

                            } else if ( javaDataType.equalsIgnoreCase("Map<Integer, String>") ) {
                                Map<Integer, String> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                        Integer.class, String.class);
                                Map<Integer, String> newData = new HashMap<>();
                                for ( Map.Entry<Integer, String> tempColEntry : tempCol.entrySet() ) {
                                    if ( tempColEntry.getKey() == Integer.parseInt(actualPrimaryKey.getColumnValueInString()) ) {
                                        logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                    } else {
                                        newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                    }
                                }
                                assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));
                            }
                        } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                            if ( javaDataType.equalsIgnoreCase("Map<String, Integer>") ) {
                                Map<String, Integer> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                        String.class, Integer.class);
                                Map<String, Integer> newData = new HashMap<>();
                                for ( Map.Entry<String, Integer> tempColEntry : tempCol.entrySet() ) {
                                    if ( tempColEntry.getKey().equalsIgnoreCase(actualPrimaryKey.getColumnValueInString()) ) {
                                        logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                    } else {
                                        newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                    }
                                }
                                assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));

                            } else if ( javaDataType.equalsIgnoreCase("Map<String, String>") ) {
                                Map<String, String> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                        String.class, String.class);
                                Map<String, String> newData = new HashMap<>();
                                for ( Map.Entry<String, String> tempColEntry : tempCol.entrySet() ) {
                                    if ( tempColEntry.getKey().equalsIgnoreCase(actualPrimaryKey.getColumnValueInString()) ) {
                                        logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                    } else {
                                        newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                    }
                                }
                                assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));
                            }
                        }
                    }
                }


            }

            Statement updateQuery = assignments.where(whereClause);

            logger.debug("### Delete query for reverse join view table " + updateQuery);
            try {
                CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), updateQuery);
            } catch ( SocketException e ) {
                logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
            }
        }
    }


    /**
     * Returns the row with existing record if there exists else returns null
     **/
    private Row getExistingRecordIfExistsInReverseJoinViewTable(PrimaryKey reverseJoinViewTablePrimaryKey) {
        Statement existingRecordQuery = null;
        // Checking if there is an already existing entry for the join key received
        if ( reverseJoinViewTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

            existingRecordQuery = QueryBuilder.select().all().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder
                    .eq(reverseJoinViewTablePrimaryKey.getColumnName(),
                            Integer.parseInt(reverseJoinViewTablePrimaryKey.getColumnValueInString())));
        } else if ( reverseJoinViewTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            existingRecordQuery = QueryBuilder.select().all().from(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).where(QueryBuilder
                    .eq(reverseJoinViewTablePrimaryKey.getColumnName(),
                            reverseJoinViewTablePrimaryKey.getColumnValueInString()));
        }

        logger.debug("#### Existing Record Query :: " + existingRecordQuery);

        List<Row> existingRows = null;
        try {
            existingRows = CassandraClientUtilities.commandExecution(CassandraClientUtilities
                    .getEth0Ip(), existingRecordQuery);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }


        if ( existingRows.size() > 0 ) {
            logger.debug("#### Existing record in reverse join view table :: " + existingRows.get(0));
            return existingRows.get(0);
        }

        return null;
    }


    /**
     * Intelligent entry of data into the reverse join table(i.e. if there is already a record then it would append to
     * the existing record...else it would make a new entry to the table)
     **/
    private void intelligentEntryPreAggViewTable(PrimaryKey reverseJoinTablePK, PrimaryKey actualPrimaryKey, TriggerRequest triggerRequest,
                                                 Map<String, List<String>> columnMap) {
        Row existingRecord = getExistingRecordIfExistsInReverseJoinViewTable(
                reverseJoinTablePK);

        if ( existingRecord == null ) {
            logger.debug("#### Fresh insertion of join key!!");
            insertIntoReverseJoinViewTable(columnMap, reverseJoinTablePK, actualPrimaryKey,
                    triggerRequest);
        } else {
            logger.debug("#### Join key already exists....will update the record!!");
            updateReverseJoinViewTable(columnMap, existingRecord, reverseJoinTablePK, actualPrimaryKey, triggerRequest);

        }

    }


    private void insertIntoReverseJoinViewTable(Map<String, List<String>> columnMap,
                                                PrimaryKey reverseJoinViewPK, PrimaryKey actualPrimaryKey, TriggerRequest triggerRequest) {

        //Note: Actual primary key - name of the column is based on the reverse join view.
        logger.debug("#### Checking || Inserting into Join View Table ::columnMap:: " + columnMap);
        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        for ( Map.Entry<String, List<String>> columnEntry : columnMap.entrySet() ) {
            // Checking for the view table primary key
            if ( columnEntry.getKey().equalsIgnoreCase(reverseJoinViewPK.getColumnName()) ) {
                columnNames.add(columnEntry.getKey());
                if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue().get(0))
                        .equalsIgnoreCase("Integer") ) {
                    objects.add(Integer.parseInt(columnEntry.getValue().get(1)));
                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue().get(0))
                        .equalsIgnoreCase("String") ) {
                    objects.add(columnEntry.getValue().get(1));
                }
            } else if ( columnEntry.getKey().equalsIgnoreCase(triggerRequest
                    .getBaseTableName() + "_" + actualPrimaryKey.getColumnName()) ) {

                if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                    List<String> tempList = new ArrayList<>();
                    tempList.add(actualPrimaryKey.getColumnValueInString());
                    columnNames.add(columnEntry.getKey());
                    objects.add(tempList);
                } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    List<Integer> tempList = new ArrayList<>();
                    tempList.add(Integer.parseInt(actualPrimaryKey.getColumnValueInString()));
                    columnNames.add(columnEntry.getKey());
                    objects.add(tempList);
                }

            } else {
                if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue()
                            .get(0)).equalsIgnoreCase("Integer") ) {
                        Map<Integer, Integer> newData = new HashMap<>();
                        newData.put(Integer.parseInt(actualPrimaryKey.getColumnValueInString()),
                                Integer.parseInt(columnEntry.getValue().get(1)));
                        columnNames.add(columnEntry.getKey());
                        objects.add(newData);
                    } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue()
                            .get(0)).equalsIgnoreCase("String") ) {
                        Map<Integer, String> newData = new HashMap<>();
                        newData.put(Integer.parseInt(actualPrimaryKey.getColumnValueInString()),
                                columnEntry.getValue().get(1));
                        columnNames.add(columnEntry.getKey());
                        objects.add(newData);
                    }
                } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                    if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue()
                            .get(0)).equalsIgnoreCase("Integer") ) {
                        Map<String, Integer> newData = new HashMap<>();
                        newData.put(actualPrimaryKey.getColumnValueInString(),
                                Integer.parseInt(columnEntry.getValue().get(1)));
                        columnNames.add(columnEntry.getKey());
                        objects.add(newData);

                    } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue()
                            .get(0)).equalsIgnoreCase("String") ) {
                        Map<String, String> newData = new HashMap<>();
                        newData.put(actualPrimaryKey.getColumnValueInString(),
                                columnEntry.getValue().get(1));
                        columnNames.add(columnEntry.getKey());
                        objects.add(newData);

                    }
                }
            }

        }
        Table reverseJoinTableConfig = operationViewTables.get(0);

        Statement finalInsertQuery = QueryBuilder.insertInto(reverseJoinTableConfig.getKeySpace(),
                reverseJoinTableConfig.getName()).values(columnNames.toArray(new String[columnNames.size()])
                , objects.toArray());
        logger.debug("### Final insert query to reverseJoin Table :: " + finalInsertQuery);

        try {
            CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), finalInsertQuery);
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }
    }


    private void updateReverseJoinViewTable(Map<String, List<String>> columnMap,
                                            Row existingRecord, PrimaryKey reverseJoinViewTablePK,
                                            PrimaryKey actualPrimaryKey, TriggerRequest triggerRequest) {

        Table reverseJoinTableConfig = operationViewTables.get(0);

        logger.debug("#### Checking ... columnMap received: {}", columnMap);

        logger.debug("#### Checking ... actualPrimaryKey:: " + actualPrimaryKey);

        logger.debug("#### Checking ... reverseJoinViewTablePK:: " + reverseJoinViewTablePK);

        logger.debug("#### Checking ... existing record received:: " + existingRecord);

        Update.Assignments assignments = QueryBuilder.update(reverseJoinTableConfig.getKeySpace(),
                reverseJoinTableConfig.getName()).with();

        Clause whereClause = null;

        for ( Map.Entry<String, List<String>> column : columnMap.entrySet() ) {

            if ( column.getKey().equalsIgnoreCase(reverseJoinViewTablePK.getColumnName()) ) {

                if ( reverseJoinViewTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    whereClause = QueryBuilder.eq(column.getKey(), Integer.parseInt(column.getValue().get(1)));
                } else if ( reverseJoinViewTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
                    whereClause = QueryBuilder.eq(column.getKey(), column.getValue().get(1));
                }

            } else if ( column.getKey().equalsIgnoreCase(triggerRequest.getBaseTableName() + "_"
                    + actualPrimaryKey.getColumnName()) ) {
                String javaDataType = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0));
                if ( javaDataType.equalsIgnoreCase("Integer") ) {
                    List<Integer> tempList = existingRecord.getList(column.getKey(), Integer.class);
                    List<Integer> newData = new ArrayList<>();
                    for ( int tempActualPK : tempList ) {
                        newData.add(tempActualPK);
                    }
                    newData.add(Integer.parseInt(actualPrimaryKey.getColumnValueInString()));
                    assignments.and(QueryBuilder.set(column.getKey(), newData));
                } else if ( javaDataType.equalsIgnoreCase("String") ) {
                    List<String> tempList = existingRecord.getList(column.getKey(), String.class);
                    List<String> newData = new ArrayList<>();
                    for ( String tempActualPK : tempList ) {
                        newData.add(tempActualPK);
                    }
                    newData.add(actualPrimaryKey.getColumnValueInString());
                    assignments.and(QueryBuilder.set(column.getKey(), newData));
                }


            } else {
                if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue()
                        .get(0)).equalsIgnoreCase("Integer") ) {

                    if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                        Map<Integer, Integer> existingData = existingRecord.getMap(column.getKey(),
                                Integer.class, Integer.class);
                        Map<Integer, Integer> newData = new HashMap<>();
                        for ( Map.Entry<Integer, Integer> row : existingData.entrySet() ) {
                            newData.put(row.getKey(), row.getValue());
                        }
                        newData.put(Integer.parseInt(actualPrimaryKey.getColumnValueInString()),
                                Integer.parseInt(column.getValue().get(1)));
                        assignments.and(QueryBuilder.set(column.getKey(), newData));

                    } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {

                        Map<String, Integer> existingData = existingRecord.getMap(column.getKey(),
                                String.class, Integer.class);
                        Map<String, Integer> newData = new HashMap<>();
                        for ( Map.Entry<String, Integer> row : existingData.entrySet() ) {
                            newData.put(row.getKey(), row.getValue());
                        }
                        newData.put(actualPrimaryKey.getColumnValueInString(),
                                Integer.parseInt(column.getValue().get(1)));
                        assignments.and(QueryBuilder.set(column.getKey(), newData));
                    }

                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue()
                        .get(0)).equalsIgnoreCase("String") ) {

                    if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                        Map<Integer, String> existingData = existingRecord.getMap(column.getKey(),
                                Integer.class, String.class);
                        Map<Integer, String> newData = new HashMap<>();
                        for ( Map.Entry<Integer, String> row : existingData.entrySet() ) {
                            newData.put(row.getKey(), row.getValue());
                        }
                        newData.put(Integer.parseInt(actualPrimaryKey.getColumnValueInString()),
                                column.getValue().get(1));
                        assignments.and(QueryBuilder.set(column.getKey(), newData));
                    } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {

                        Map<String, String> existingData = existingRecord.getMap(column.getKey(),
                                String.class, String.class);
                        Map<String, String> newData = new HashMap<>();
                        for ( Map.Entry<String, String> row : existingData.entrySet() ) {
                            newData.put(row.getKey(), row.getValue());
                        }
                        newData.put(actualPrimaryKey.getColumnValueInString(),
                                column.getValue().get(1));
                        assignments.and(QueryBuilder.set(column.getKey(), newData));
                    }
                }
            }

        }

        Statement updateQuery = assignments.where(whereClause);

        logger.debug("### Final updateQuery for reverse join :: " + updateQuery);

        try {
            CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), updateQuery.toString());
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

        logger.debug("##### Entering delete trigger for ReverseJoin Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure :: {}", this.inputViewTables);

        PrimaryKey reverseJoinPrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableConfigWithoutValue(
                operationViewTables.get(0).getKeySpace(), operationViewTables.get(0).getName());

        if ( reverseJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            reverseJoinPrimaryKey.setColumnValueInString(deltaTableRecord.getInt(reverseJoinPrimaryKey.getColumnName()
                    + DeltaViewTrigger.CURRENT) + "");
        } else if ( reverseJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            reverseJoinPrimaryKey.setColumnValueInString(deltaTableRecord.getString(reverseJoinPrimaryKey.getColumnName()
                    + DeltaViewTrigger.CURRENT));
        }

        logger.debug("#### reverseJoinPrimaryKey :: " + reverseJoinPrimaryKey);

        Row existingRowReverseJoinView = null;
        try {
            existingRowReverseJoinView = ViewMaintenanceUtilities.getExistingRecordIfExists(reverseJoinPrimaryKey,
                    operationViewTables.get(0));
        } catch ( SocketException e ) {
            logger.error("Error!!! " + ViewMaintenanceUtilities.getStackTrace(e));
        }

        logger.debug("#### ExistingRowReverseJoinView :: " + existingRowReverseJoinView);


        if ( inputViewTables.get(0).getName().contains(WHERE_TABLE_INDENTIFIER) ) {
            Table whereTableConfig = ViewMaintenanceUtilities.getConcernedWhereTableFromWhereTablesList(triggerRequest,
                    inputViewTables);

            logger.debug("#### Checking wheretableconfig :: " + whereTableConfig);

            if ( whereTableConfig != null ) {
                PrimaryKey whereTablePrimaryKey = ViewMaintenanceUtilities.getPrimaryKeyFromTableConfigWithoutValue(whereTableConfig
                        .getKeySpace(), whereTableConfig.getName());

                String[] whereStringArr = triggerRequest.getWhereString().split("=");
                logger.debug("#### Checking :: whereStringArr :: " + whereStringArr);
                logger.debug("#### Checking :: whereStringArr length:: " + whereStringArr.length);
                if ( whereTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    whereTablePrimaryKey.setColumnValueInString(whereStringArr[1].trim());
                } else if ( whereTablePrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                    whereTablePrimaryKey.setColumnValueInString(whereStringArr[whereStringArr.length].trim()
                            .replaceAll("'", ""));
                }

                logger.debug("#### whereTablePrimaryKey :: " + whereTablePrimaryKey);

                if ( existingRowReverseJoinView != null ) {

                    Map<String, List<String>> reverseColumnMap = getCurrReverseJoinColumnMapFromDelta();
                    logger.debug("#### ReverseJoin ColumnMap :: " + reverseColumnMap);

                    deleteFromReverseViewTable(reverseJoinPrimaryKey, whereTablePrimaryKey,
                            triggerRequest, reverseColumnMap);
                }
            }
        }
        return true;
    }

    public static ReverseJoinOperation getInstance(List<Table> inputViewTable,
                                                   List<Table> operationViewTable) {
        ReverseJoinOperation reverseJoinOperation = new ReverseJoinOperation();
        reverseJoinOperation.setInputViewTable(inputViewTable);
        reverseJoinOperation.setOperationViewTables(operationViewTable);
        return reverseJoinOperation;
    }

    @Override
    public String toString() {
        return "ReverseJoinOperation{" +
                ",\n inputViewTable=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }


    private Map<String, List<String>> getCurrReverseJoinColumnMapFromDelta() {
        Map<String, List<String>> columnMap = new HashMap<>();
        // Format::
        // Key: Name of the column
        // Value : CassandraInternalType, ValueInString(not mandatory), isPrimaryKeyForReverseJoinTable(not mandatory)
        Map<String, ColumnDefinition> reverseJoinDesc = ViewMaintenanceUtilities.getTableDefinitition(
                operationViewTables.get(0).getKeySpace(), operationViewTables.get(0).getName());

        for ( Map.Entry<String, ColumnDefinition> reverseJoinColEntry : reverseJoinDesc.entrySet() ) {
            String javaDataType = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(reverseJoinColEntry
                    .getValue().type.toString());
            List<String> tempList = new ArrayList<>();
            tempList.add(reverseJoinColEntry.getValue().type.toString());
            if ( reverseJoinColEntry.getValue().isPartitionKey() ) {
                if ( javaDataType.equalsIgnoreCase("Integer") ) {
                    tempList.add(deltaTableRecord.getInt(reverseJoinColEntry.getValue().name.toString() +
                            DeltaViewTrigger.CURRENT) + "");
                } else if ( javaDataType.equalsIgnoreCase("String") ) {
                    tempList.add(deltaTableRecord.getString(reverseJoinColEntry.getValue().name.toString() +
                            DeltaViewTrigger.CURRENT));
                }

                tempList.add("true");
            }
            columnMap.put(reverseJoinColEntry.getKey(), tempList);

        }
        return columnMap;
    }
}
