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
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.tools.NodeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 8/15/15.
 */

public class ReverseJoinOperation extends GenericOperation {
    private static final Logger logger = LoggerFactory.getLogger(ReverseJoinOperation.class);
    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private List<Table> inputViewTable;
    private List<Table> operationViewTables;
    private Table viewConfig;
    private List<Join> joins;

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
        return inputViewTable;
    }

    public void setInputViewTable(List<Table> inputViewTable) {
        this.inputViewTable = inputViewTable;
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
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        EqualsTo onExpresssionJoin = (EqualsTo) joins.get(0).getOnExpression();
        List<String> tablesInvolved = viewConfig.getRefBaseTables();
        boolean isResultSuccessful = false;
        Statement existingRecordQuery = null;
        String primaryColName = ((Column) onExpresssionJoin.getLeftExpression()).getColumnName(); //Primary col of the view table
        String leftTableName = ((Column) onExpresssionJoin.getLeftExpression()).getTable().getName();
        String rightTableName = ((Column) onExpresssionJoin.getRightExpression()).getTable().getName();
        Map<String, Map<String, ColumnDefinition>> tableDesc = new HashMap<>();
        String primaryValue = "";
        PrimaryKey actualPrimaryKey = new PrimaryKey();
        PrimaryKey reverseJoinViewTablePrimaryKey = new PrimaryKey();
        Table whereTableInvolved = null;

        // Getting the concerned whereViewTable involved
        for ( Table whereTable : inputViewTable ) {
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
                    List<String> tempColDescList = new ArrayList<>();
                    String tempDataKey = (String) dataIter.next();
                    logger.debug("Key: " + tempDataKey);
                    logger.debug("Value: " + dataJson.get(tempDataKey));
                    tempColDescList.add(table.getValue().get(tempDataKey).type.toString()); // Adding Cassandra internal type of the column
                    if ( tempDataKey.equalsIgnoreCase(primaryColName) ) {
                        primaryValue = ((String) dataJson.get(tempDataKey)).replaceAll("'", "");
                        tempColDescList.add(primaryValue);
                        tempColDescList.add("true");
                        columnMap.put(tempDataKey, tempColDescList);
                        reverseJoinViewTablePrimaryKey.setColumnInternalCassType(table.getValue().get(tempDataKey).type.toString());
                        reverseJoinViewTablePrimaryKey.setColumnJavaType(ViewMaintenanceUtilities
                                .getJavaTypeFromCassandraType(reverseJoinViewTablePrimaryKey.getColumnInternalCassType()));
                        reverseJoinViewTablePrimaryKey.setColumnValueInString(primaryValue);
                        reverseJoinViewTablePrimaryKey.setColumnName(primaryColName);
                    } else {
                        tempColDescList.add(table.getValue().get(tempDataKey).type.toString());
                        tempColDescList.add(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        tempColDescList.add("false");
                        if ( ViewMaintenanceUtilities.checkPresenceOfColumnInDifferentTable(table.getKey(),
                                tempDataKey, tableDesc) ) {
                            columnMap.put(tempDataKey + "_" + triggerRequest.getBaseTableName(), tempColDescList);
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

        // Select from the where table to see whether this record which needs to be maintained

        List<Row> viewEligibilityResults = null;
        if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            Statement viewMaintenanceEligibilityQuery = QueryBuilder.select().all().from(whereTableInvolved.getKeySpace(),
                    whereTableInvolved.getName()).where(QueryBuilder.eq(actualPrimaryKey.getColumnName(),
                    Integer.parseInt(actualPrimaryKey.getColumnValueInString())));
            logger.debug("### ViewMaintenance eligibility query(Integer) :: " + viewMaintenanceEligibilityQuery);
            viewEligibilityResults = CassandraClientUtilities.commandExecution("localhost",
                    viewMaintenanceEligibilityQuery);
        } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            Statement viewMaintenanceEligibilityQuery = QueryBuilder.select().all().from(whereTableInvolved.getKeySpace(),
                    whereTableInvolved.getName()).where(QueryBuilder.eq(actualPrimaryKey.getColumnName(),
                    actualPrimaryKey.getColumnValueInString()));
            logger.debug("### ViewMaintenance eligibility query(String) :: " + viewMaintenanceEligibilityQuery);

            viewEligibilityResults = CassandraClientUtilities.commandExecution("localhost",
                    viewMaintenanceEligibilityQuery);
        }

        if ( viewEligibilityResults.size() == 0 ) {
            logger.debug("#### viewEligibilityResults are nil hence reverse join view maintenance is not required!!");
            return true;
        } else {
            logger.debug("### viewEligibility Results :: " + viewEligibilityResults.get(0));

            logger.debug("### Reverse Join View Maintenance starts ### ");
        }


//        List<ColumnDefinitions.Definition> columnDefinitions = deltaTableRecord.getColumnDefinitions().asList();
        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnMap.get(primaryColName).get(0))
                .equalsIgnoreCase("Integer") ) {

            if ( deltaTableRecord.getInt(primaryColName + DeltaViewTrigger.LAST) == 0 ) {

                // Checking if there is an already existing entry for the join key received
                Row existingRecord = getExistingRecordIfExistsInReverseJoinViewTable(reverseJoinViewTablePrimaryKey);
                if ( existingRecord != null ) {
                    logger.debug("### Existing record in reverse join viewtable ## " +
                            "{} is ::{}", operationViewTables.get(0).getKeySpace() + "." +
                            operationViewTables.get(0).getName(), existingRecord.toString());
                     // Update the already existing record with the new record
                    updateReverseJoinViewTable(columnMap, operationViewTables.get(0), existingRecord,
                            reverseJoinViewTablePrimaryKey, actualPrimaryKey);

                } else {
                    // Need a new entry in the view table
                    insertIntoReverseJoinViewTable(columnMap, operationViewTables.get(0), reverseJoinViewTablePrimaryKey,
                            actualPrimaryKey);
                }


            } else {

                // There exists an old entry for the primary key in the delta view

                if ( deltaTableRecord.getInt(primaryColName + DeltaViewTrigger.CURRENT) ==
                        deltaTableRecord.getInt(primaryColName + DeltaViewTrigger.LAST) ) {
                    // Already an entry exists in the reverse join view with the same join key
                    Row existingRecord = getExistingRecordIfExistsInReverseJoinViewTable(reverseJoinViewTablePrimaryKey);
                    if ( existingRecord != null ) {
                        logger.debug("### Existing record in table:{} is ::{}", operationViewTables.get(0).getKeySpace() + "." +
                                operationViewTables.get(0).getName(), existingRecord.toString());
                        updateReverseJoinViewTable(columnMap, operationViewTables.get(0), existingRecord,
                                reverseJoinViewTablePrimaryKey, actualPrimaryKey);

                    }

                } else {
                    // Already an entry exists in the reverse join view with the different join key
                    // Delete the old entry in the reverse join view
                    // Insert/Update a new entry in the reverse join view
                    Row existingRecord = getExistingRecordIfExistsInReverseJoinViewTable(reverseJoinViewTablePrimaryKey);
                    if ( existingRecord != null ) {
                        logger.debug("### Existing record in table:{} is ::{}", operationViewTables.get(0).getKeySpace() + "." +
                                operationViewTables.get(0).getName(), existingRecord.toString());

                        // Update the reverse join view
                        updateReverseJoinViewTable(columnMap, operationViewTables.get(0), existingRecord,
                                reverseJoinViewTablePrimaryKey, actualPrimaryKey);
                    } else {
                        // There is no such key for the curr join key
                        // Fresh insertion of the record required

                        insertIntoReverseJoinViewTable(columnMap, operationViewTables.get(0), reverseJoinViewTablePrimaryKey,
                                actualPrimaryKey);
                    }

                    PrimaryKey oldReverseJoinPrimaryKey = new PrimaryKey();
                    oldReverseJoinPrimaryKey.setColumnName(actualPrimaryKey.getColumnName());
                    oldReverseJoinPrimaryKey.setColumnValueInString(deltaTableRecord
                            .getInt(primaryColName + DeltaViewTrigger.LAST) + "");
                    oldReverseJoinPrimaryKey.setColumnJavaType(actualPrimaryKey.getColumnJavaType());
                    oldReverseJoinPrimaryKey.setColumnInternalCassType(actualPrimaryKey.getColumnInternalCassType());

                    deleteFromReverseViewTable(oldReverseJoinPrimaryKey, actualPrimaryKey);
                    logger.debug("### Existing Record Query for curr joinkey ::" + existingRecordQuery);
                }


            }

        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnMap.get(primaryColName).get(0))
                .equalsIgnoreCase("String") ) {
            System.out.println("");
        }


        return false;
    }

    private void deleteFromReverseViewTable(PrimaryKey oldReverseJoinPrimaryKey, PrimaryKey actualPrimaryKey) {
        Row existingRecord = getExistingRecordIfExistsInReverseJoinViewTable(oldReverseJoinPrimaryKey);
//        StringBuffer deleteQueryReverseJoinViewTable = new StringBuffer("update " + operationViewTables.get(0)
//                .getKeySpace() + "." + operationViewTables.get(0).getName() + " ( ");

        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Clause whereClause = null;
//        StringBuffer valuesPart = new StringBuffer("values (");
        List<QueryBuilder> updateList = new ArrayList<>();
        if ( existingRecord != null ) {
            Map<String, ColumnDefinition> tableDesc = ViewMaintenanceUtilities.getTableDefinitition(operationViewTables
                    .get(0).getKeySpace(), operationViewTables.get(0).getName());

            for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : tableDesc.entrySet() ) {

                if ( oldReverseJoinPrimaryKey.getColumnName().equalsIgnoreCase(columnDefinitionEntry.getKey()) ) {
                    if (oldReverseJoinPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer")) {
                        whereClause = QueryBuilder.eq(oldReverseJoinPrimaryKey.getColumnName(),
                                Integer.parseInt(oldReverseJoinPrimaryKey.getColumnValueInString()));
                    } else if (actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String")) {
                        whereClause = QueryBuilder.eq(oldReverseJoinPrimaryKey.getColumnName(),
                                oldReverseJoinPrimaryKey.getColumnValueInString());
                    }

                } else {
                    if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue().type
                                .toString()).equalsIgnoreCase("Integer") ) {
                            Map<Integer, Integer> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                    Integer.class, Integer.class);
                            Map<Integer, Integer> newData = new HashMap<>();
                            for ( Map.Entry<Integer, Integer> tempColEntry : tempCol.entrySet() ) {
                                if ( tempColEntry.getValue() == Integer.parseInt(
                                        oldReverseJoinPrimaryKey.getColumnValueInString()) ) {
                                    logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                } else {
                                    newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                }
                            }
                            assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));

                        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue().type
                                .toString()).equalsIgnoreCase("String") ) {
                            Map<Integer, String> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                    Integer.class, String.class);
                            Map<Integer, String> newData = new HashMap<>();
                            for ( Map.Entry<Integer, String> tempColEntry : tempCol.entrySet() ) {
                                if ( tempColEntry.getValue().equalsIgnoreCase(oldReverseJoinPrimaryKey.getColumnValueInString())) {
                                    logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                } else {
                                    newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                }
                            }
                            assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));
                        }
                    } else if ( actualPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue().type
                                .toString()).equalsIgnoreCase("Integer") ) {
                            Map<String, Integer> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                    String.class, Integer.class);
                            Map<String, Integer> newData = new HashMap<>();
                            for ( Map.Entry<String, Integer> tempColEntry : tempCol.entrySet() ) {
                                if ( tempColEntry.getValue() == Integer.parseInt(oldReverseJoinPrimaryKey.getColumnValueInString())) {
                                    logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                } else {
                                    newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                }
                            }
                            assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));

                        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinitionEntry.getValue().type
                                .toString()).equalsIgnoreCase("String") ) {
                            Map<String, String> tempCol = existingRecord.getMap(columnDefinitionEntry.getKey(),
                                    String.class, String.class);
                            Map<String, String> newData = new HashMap<>();
                            for ( Map.Entry<String, String> tempColEntry : tempCol.entrySet() ) {
                                if ( tempColEntry.getValue().equalsIgnoreCase(oldReverseJoinPrimaryKey.getColumnValueInString())) {
                                    logger.debug("### Escaped and not added to the result map :: " + tempColEntry);
                                } else {
                                    newData.put(tempColEntry.getKey(), tempColEntry.getValue());
                                }
                            }
                            assignments.and(QueryBuilder.set(columnDefinitionEntry.getKey(), newData));
                        }
                    }
                }

                Statement updateQuery = assignments.where(whereClause);

                logger.debug("### Delete query for reverse join view table " + updateQuery);
//                CassandraClientUtilities.commandExecution("localhost", updateQuery);

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

        List<Row> existingRows = CassandraClientUtilities.commandExecution("localhost", existingRecordQuery);


        if ( existingRows.size() > 0 ) {
            logger.debug("#### Existing record in reverse join view table :: " + existingRows.get(0));
            return existingRows.get(0);
        }

        return null;
    }


    private void insertIntoReverseJoinViewTable(Map<String, List<String>> columnMap, Table reverseJoinTableConfig,
                                                PrimaryKey reverseJoinViewPK, PrimaryKey actualPrimaryKey) {
//        StringBuffer insertQuery = new StringBuffer("insert into " + reverseJoinTableConfig.getKeySpace() + "." +
//                reverseJoinTableConfig.getName() + "(");
//        StringBuffer valuesPart = new StringBuffer("values ( ");

        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        for ( Map.Entry<String, List<String>> columnEntry : columnMap.entrySet() ) {
            // Checking for the view table primary key
            if ( columnEntry.getKey().equalsIgnoreCase(reverseJoinViewPK.getColumnName()) ) {
                if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue().get(0))
                        .equalsIgnoreCase("Integer") ) {
                    columnNames.add(columnEntry.getKey());
                    objects.add(columnEntry.getValue().get(1));
                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnEntry.getValue().get(0))
                        .equalsIgnoreCase("String") ) {
                    columnNames.add(columnEntry.getKey());
                    objects.add(columnEntry.getValue().get(1));
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

        Statement finalInsertQuery = QueryBuilder.insertInto(reverseJoinTableConfig.getKeySpace(),
                reverseJoinTableConfig.getName()).values((String[])columnNames.toArray(), objects.toArray());
        logger.debug("### Final insert query :: " + finalInsertQuery);

//        CassandraClientUtilities.commandExecution("localhost", finalInsertQuery);
    }


    private static void updateReverseJoinViewTable(Map<String, List<String>> columnMap, Table reverseJoinTableConfig,
                                                   Row existingRecord, PrimaryKey reverseJoinViewTablePK,
                                                   PrimaryKey actualPrimaryKey) {

//        StringBuffer updateQuery = new StringBuffer("update " + reverseJoinTableConfig.getKeySpace() + "." +
//                reverseJoinTableConfig.getName() + "set ");

        Update.Assignments assignments = QueryBuilder.update(reverseJoinTableConfig.getKeySpace(),
                reverseJoinTableConfig.getName()).with();

        Clause whereClause = null;
//        StringBuffer whereString = new StringBuffer(" where ");

        for ( Map.Entry<String, List<String>> column : columnMap.entrySet() ) {

            if ( column.getKey().equalsIgnoreCase(reverseJoinViewTablePK.getColumnName()) ) {

                if ( reverseJoinViewTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                    whereClause = QueryBuilder.eq(column.getKey(), Integer.parseInt(column.getValue().get(1)));
                } else if ( reverseJoinViewTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
                    whereClause = QueryBuilder.eq(column.getKey(), column.getValue().get(1));
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

            Statement updateQuery = assignments.where(whereClause);

            logger.debug("### Final updateQuery for reverse join :: " + updateQuery);

//            CassandraClientUtilities.commandExecution("localhost", updateQuery.toString());

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
                ",\n inputViewTable=" + inputViewTable +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }
}
