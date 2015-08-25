package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
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

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public static WhereOperation getInstance(Row deltaTableRecord, List<Table> inputViewTable,
                                             List<Table> operationViewTable) {
        WhereOperation whereOperation = new WhereOperation();
        whereOperation.setDeltaTableRecord(deltaTableRecord);
//        whereOperation.setInputViewTable(inputViewTable);
        whereOperation.setOperationViewTable(operationViewTable);
        return whereOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering insert trigger for Where Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Trigger request :: " + triggerRequest);
        Statement fetchExistingRow = null;
        boolean isResultSuccessful = false;
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

        boolean deciderForNewRow = false; // If true then the stream of information needs to stored else deleted if already present

        Map<String, List<String>> columnMap = new HashMap<>();
        String viewTableName = TABLE_PREFIX + triggerRequest.getBaseTableName();
        logger.debug("### Checking --- viewTableName  :: " + viewTableName);
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        String primaryKey = null;
        while ( dataIter.hasNext() ) {
            String tempDataKey = (String) dataIter.next();
            logger.debug("Key: " + tempDataKey);
            logger.debug("Value: " + dataJson.get(tempDataKey));
            for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : baseTableDesc.entrySet() ) {
                ColumnDefinition columnDefinition = columnDefinitionEntry.getValue();
                if ( tempDataKey.equalsIgnoreCase(columnDefinition.name.toString()) ) {
                    List<String> tempList = new ArrayList<>(); // Format of storing: internalcassandra type, value, isPrimaryKey

                    tempList.add(columnDefinition.type.toString());

                    if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                            columnDefinition.type.toString()).equalsIgnoreCase("String") ) {
                        tempList.add(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        if ( columnDefinition.isPartitionKey() ) {
                            fetchExistingRow = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewTableName)
                                    .where(QueryBuilder.eq(columnDefinition.name.toString(),
                                            ((String) dataJson.get(tempDataKey)).replaceAll("'", "")));
                        }
                    } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                            columnDefinition.type.toString()).equalsIgnoreCase("Integer") ) {
                        tempList.add((String) dataJson.get(tempDataKey));
                        if ( columnDefinition.isPartitionKey() ) {
                            fetchExistingRow = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewTableName)
                                    .where(QueryBuilder.eq(columnDefinition.name.toString(),
                                            Integer.parseInt(((String) dataJson.get(tempDataKey)))));
                        }
                    }
                    tempList.add(columnDefinition.isPartitionKey() ? "true" : "false");

                    columnMap.put(tempDataKey, tempList);
                    if ( columnDefinition.isPartitionKey() ) {
                        primaryKey = tempDataKey;

                    }
                }
            }

        }


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
                        Column column = getColumnObject(expression);
                        logger.debug("### Evaluating expression :: " + expression.toString());
                        if ( column.getTable().getName().equalsIgnoreCase(targetTableDerivedFromOperationTable) ) {
                            logger.debug("### Concerned expression :: " + expression.toString() + "for base table :: "
                                    + targetTableDerivedFromOperationTable);
                            logger.debug("### Fetch Existing rows query ::" + fetchExistingRow);
                            // Fetching existing record from the view table if exists
                            List<Row> existingRecords = CassandraClientUtilities.commandExecution("localhost",
                                    fetchExistingRow);
                            if ( existingRecords.size() < 0 ) {
                                Row existingRecord = existingRecords.get(0);
                                logger.debug("### Existing record ### " + existingRecord);

                                if ( checkExpression(whereExpression, columnMap) ) {
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
                                if ( checkExpression(whereExpression, columnMap) ) {

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
                        Column column = getColumnObject(expression);
                    }
                } else if ( whereExpressions.size() == 1 ) {
                    logger.debug("### Evaluating expression :: " + whereExpression.toString());
                    logger.debug("### Fetch Existing rows query ::" + fetchExistingRow);
                    Column column = getColumnObject(whereExpression);
                    if ( column.getTable().getName().equalsIgnoreCase(targetTableDerivedFromOperationTable) ) {

                        List<Row> existingRecords = CassandraClientUtilities.commandExecution("localhost",
                                fetchExistingRow);
                        if ( existingRecords != null && existingRecords.size() > 0 ) {
                            Row existingRecord = existingRecords.get(0);
                            logger.debug("### Existing record ### " + existingRecord);

                            if ( checkExpression(whereExpression, columnMap) ) {
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
                            if ( checkExpression(whereExpression, columnMap) ) {
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


    private boolean checkExpression(Expression whereExpression, Map<String, List<String>> columnMap) {

        logger.debug("### Checking -- Inside checkExpression :: Column map : " + columnMap);
        boolean result = false;
        Column column = null;
        String colName = "";
        String rightExpression = "";
        if ( whereExpression instanceof MinorThan ) {
            column = ((Column) ((MinorThan) whereExpression).getLeftExpression());
//            tableName = column.getTable().getName();
            colName = column.getColumnName();
            List<String> userData = columnMap.get(colName);
            rightExpression = ((MinorThan) whereExpression).getRightExpression().toString();
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase("Integer") ) {
                if ( Integer.parseInt(userData.get(1)) < Integer.parseInt(rightExpression) ) {
                    result = true;
                }
            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase(("String")) ) {
                //TODO: Need to implement
            }
        } else if ( whereExpression instanceof MinorThanEquals ) {
            column = ((Column) ((MinorThanEquals) whereExpression).getLeftExpression());
//            tableName = column.getTable().getName();
            colName = column.getColumnName();
            List<String> userData = columnMap.get(colName);
            rightExpression = ((MinorThanEquals) whereExpression).getRightExpression().toString();
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase("Integer") ) {
                if ( Integer.parseInt(userData.get(1)) <= Integer.parseInt(rightExpression) ) {
                    result = true;
                }
            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase(("String")) ) {
                //TODO: Need to implement
            }
        } else if ( whereExpression instanceof GreaterThan ) {
            column = ((Column) ((GreaterThan) whereExpression).getLeftExpression());
//            tableName = column.getTable().getName();
            colName = column.getColumnName();
            List<String> userData = columnMap.get(colName);
            logger.debug("### Checking -- colName :: " + colName);
            rightExpression = ((GreaterThan) whereExpression).getRightExpression().toString();
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase("Integer") ) {
                logger.debug("### Checking userdata.get(1)= {} and rightExpression = {} ", userData.get(1), rightExpression);
                if ( Integer.parseInt(userData.get(1)) > Integer.parseInt(rightExpression) ) {
                    result = true;
                }
            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase(("String")) ) {
                //TODO: Need to implement
            }
        } else if ( whereExpression instanceof GreaterThanEquals ) {
            column = ((Column) ((GreaterThanEquals) whereExpression).getLeftExpression());
//            tableName = column.getTable().getName();
            colName = column.getColumnName();
            List<String> userData = columnMap.get(colName);
            rightExpression = ((GreaterThanEquals) whereExpression).getRightExpression().toString();
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase("Integer") ) {
                if ( Integer.parseInt(userData.get(1)) >= Integer.parseInt(rightExpression) ) {
                    result = true;
                }
            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase(("String")) ) {
                //TODO: Need to implement
            }
        }
        logger.debug("### Result for checkExpression() " + result);
        return result;
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

        CassandraClientUtilities.commandExecution("localhost", insertQuery);
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
                    whereStr = " where " + column.getKey() + "= " + column.getValue().get(1) + ", ";
                } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                        .equalsIgnoreCase("String") ) {
                    whereStr = " where " + column.getKey() + "= '" + column.getValue().get(1) + "', ";
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

            updateQuery = ViewMaintenanceUtilities.removesCommaSpace(updateQuery);
            updateQuery.append(whereStr);
            logger.debug("### Final update query to where view table :: " + updateQuery.toString());

//            CassandraClientUtilities.commandExecution("localhost", updateQuery.toString());

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

        CassandraClientUtilities.commandExecution("localhost", statement);


    }


    private Column getColumnObject(Expression expression) {
        net.sf.jsqlparser.schema.Column column = null;
        if ( expression instanceof MinorThan ) {
            column = ((Column) ((MinorThan) expression).getLeftExpression());

        } else if ( expression instanceof GreaterThan ) {
            column = ((Column) ((GreaterThan) expression).getLeftExpression());

        } else if ( expression instanceof MinorThanEquals ) {
            column = ((Column) ((MinorThanEquals) expression).getLeftExpression());

        } else if ( expression instanceof GreaterThanEquals ) {
            column = ((Column) ((GreaterThanEquals) expression).getLeftExpression());

        } else if ( expression instanceof EqualsTo ) {
            column = ((Column) ((EqualsTo) expression).getLeftExpression());
        }
        return column;
    }

    @Override
    public boolean updateTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public boolean deleteTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public String toString() {
        return "WhereOperation{" +
                "\n deltaTableRecord=" + deltaTableRecord +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }
}
