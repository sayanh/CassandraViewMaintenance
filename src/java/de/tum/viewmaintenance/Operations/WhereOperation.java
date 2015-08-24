package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
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
        TABLE_PREFIX = viewConfig + "_where_";
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
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
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

        boolean deciderForNewRow = false; // If true then the stream of information needs to stored else deleted if already present

        Map<String, List<String>> columnMap = new HashMap<>();
        String viewTableName = TABLE_PREFIX + triggerRequest.getBaseTableName();
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
                        primaryKey = (String) tempList.get(1);

                    }
                }
            }

        }


        logger.debug("### Stream available for maintenance :: " + columnMap);


        for ( Table whereTable : operationViewTables ) {

            String targetWhereTable = whereTable.getName();
            String targetWhereTableArr[] = targetWhereTable.split("_");
            String targetTableDerivedFromOperationTables = targetWhereTableArr[2];
            if ( targetTableDerivedFromOperationTables.equalsIgnoreCase(triggerRequest.getBaseTableName()) ) {
                if ( whereExpression instanceof AndExpression ) {
                    for ( Expression expression : whereExpressions ) {
                        Column column = getColumnObject(expression);
                        logger.debug("### Evaluating expression :: " + expression.toString());
                        if ( column.getTable().getName().equalsIgnoreCase(targetTableDerivedFromOperationTables) ) {
                            logger.debug("### Concerned expression :: " + expression.toString() + "for base table :: "
                                    + targetTableDerivedFromOperationTables);
                            // Fetching existing record from the view table if exists
                            List<Row> existingRecords = CassandraClientUtilities.commandExecution("localhost",
                                    fetchExistingRow);
                            if ( existingRecords.size() < 0 ) {
                                Row existingRecord = existingRecords.get(0);
                                logger.debug("### Existing record ### " + existingRecord);

                                if ( checkExpression(whereExpression, columnMap) ) {
                                    // Update if exists
                                    updateIntoWhereViewTable(columnMap, whereTable);
                                } else {
                                    String primaryKeyType = columnMap.get(primaryKey).get(0);
                                    String primaryValue = columnMap.get(primaryKey).get(1);
                                    deleteFromWhereViewTable(primaryKey, primaryKeyType, primaryValue, whereTable);
                                }
                            } else {
                                // Insert - This is a new entry
                                if ( checkExpression(whereExpression, columnMap) )
                                    insertIntoWhereViewTable(columnMap, whereTable);
                            }
                        }
                    }
                } else if ( whereExpression instanceof OrExpression ) {
                    for ( Expression expression : whereExpressions ) {
                        Column column = getColumnObject(expression);
                    }
                }

                break;
            }

        }

        return true;

    }


    private boolean checkExpression(Expression whereExpression, Map<String, List<String>> columnMap) {
        boolean result = false;
        Column column = null;
        String comparisonStr = "";
        String tableName = "";
        String colName = "";
        String rightExpression = "";
        if ( whereExpression instanceof MinorThan ) {
            column = ((Column) ((MinorThan) whereExpression).getLeftExpression());
            List<String> userData = columnMap.get(colName);
            tableName = column.getTable().getName();
            colName = column.getColumnName();
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
            List<String> userData = columnMap.get(colName);
            tableName = column.getTable().getName();
            colName = column.getColumnName();
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
            List<String> userData = columnMap.get(colName);
            tableName = column.getTable().getName();
            colName = column.getColumnName();
            rightExpression = ((GreaterThan) whereExpression).getRightExpression().toString();
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase("Integer") ) {
                if ( Integer.parseInt(userData.get(1)) > Integer.parseInt(rightExpression) ) {
                    result = true;
                }
            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(userData.get(0))
                    .equalsIgnoreCase(("String")) ) {
                //TODO: Need to implement
            }
        } else if ( whereExpression instanceof GreaterThanEquals ) {
            column = ((Column) ((GreaterThanEquals) whereExpression).getLeftExpression());
            List<String> userData = columnMap.get(colName);
            tableName = column.getTable().getName();
            colName = column.getColumnName();
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

        return result;
    }

    private void insertIntoWhereViewTable(Map<String, List<String>> columnMap, Table whereTable) {

        StringBuffer insertStatement = new StringBuffer("Insert into " + whereTable.getKeySpace()
                + "." + whereTable.getName() + " ( ");
        StringBuffer valuesPart = new StringBuffer("values (");
        for ( Map.Entry<String, List<String>> column : columnMap.entrySet() ) {
            insertStatement.append(column.getKey() + ", ");
            if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                    .equalsIgnoreCase("Integer") ) {
                valuesPart.append(column.getValue().get(1));

            } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(column.getValue().get(0))
                    .equalsIgnoreCase("String") ) {

                valuesPart.append("'" + column.getValue().get(1) + "', ");
            }
        }

        String finalInsertQuery = ViewMaintenanceUtilities.joinInsertAndValues(insertStatement, valuesPart);

        logger.debug("### Final insert query to where view table:: " + finalInsertQuery);

//        CassandraClientUtilities.commandExecution("localhost", finalInsertQuery);
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

    private void deleteFromWhereViewTable(String primaryKeyColName, String primaryKeyType, String primaryKeyValue, Table whereTable) {
        Statement statement = null;
        if (ViewMaintenanceUtilities.getJavaTypeFromCassandraType(primaryKeyType).equalsIgnoreCase("Integer")) {
            statement = QueryBuilder.delete().from(whereTable.getKeySpace(),
                    whereTable.getName()).where(QueryBuilder.eq(primaryKeyColName, Integer.parseInt(primaryKeyValue)));
        } else if (ViewMaintenanceUtilities.getJavaTypeFromCassandraType(primaryKeyType).equalsIgnoreCase("String")) {
            statement = QueryBuilder.delete().from(whereTable.getKeySpace(),
                    whereTable.getName()).where(QueryBuilder.eq(primaryKeyColName, primaryKeyValue));
        }


        logger.debug("### Final delete query for where view table :: " + statement.toString());

//        CassandraClientUtilities.commandExecution("localhost", statement);



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
