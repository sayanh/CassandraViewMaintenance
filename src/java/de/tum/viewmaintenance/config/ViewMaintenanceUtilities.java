package de.tum.viewmaintenance.config;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by shazra on 6/27/15.
 * <p/>
 * This class contains various utilities pertaining to the view maintenance.
 */
public final class ViewMaintenanceUtilities {
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceUtilities.class);

    public static Map<String, ColumnDefinition> getTableDefinitition(String keyspaceName, String tableName) {
        Map<String, ColumnDefinition> tableStrucMap = new HashMap<>();
        // Getting the CFMetadata for a particular table
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(keyspaceName, tableName);
        Collection<ColumnDefinition> columnFamilyCollection = cfMetaData.allColumns();
        for ( ColumnDefinition columnDefinition : columnFamilyCollection ) {
//            logger.debug("ViewMaintenanceUtilities | Column Definition : {}", columnDefinition);
            tableStrucMap.put(columnDefinition.name + "", columnDefinition);
        }

//        logger.debug("The Map of the table::{} definition ={}", keyspaceName + "." + tableName, tableStrucMap);
        return tableStrucMap;
    }


    /**
     * It returns an equivalent Java datatype for an entered Cassandra type
     **/
    public static String getJavaTypeFromCassandraType(String cassandraType) {
        String javaType = "";
        logger.debug(" The cassandra type received is " + cassandraType);

        if ( cassandraType.equalsIgnoreCase("org.apache.cassandra.db.marshal.UTF8Type") ) {
            javaType = "String";
        } else if ( cassandraType.equalsIgnoreCase("org.apache.cassandra.db.marshal.Int32Type") ) {
            javaType = "Integer";
        }
        return javaType;
    }

    /**
     * It returns an equivalent CQL3 data type from Cassandra's internal data type
     **/
    public static String getCQL3DataTypeFromCassandraInternalDataType(String internalDataType) {
        String cql3Type = "";
        logger.debug(" The cassandra type received is " + internalDataType);

        if ( internalDataType.equalsIgnoreCase("org.apache.cassandra.db.marshal.UTF8Type") ) {
            cql3Type = "text";
        } else if ( internalDataType.equalsIgnoreCase("org.apache.cassandra.db.marshal.Int32Type") ) {
            cql3Type = "int";
        } else if ( internalDataType.equalsIgnoreCase("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)") ) {
            cql3Type = "map <int, text>";
        }
        return cql3Type;
    }


    /**
     * It returns an equivalent Cassandra internal data type from CQL3 data type
     **/
    public static String getCassInternalDataTypeFromCQL3DataType(String cql3DataType) {
        String cassInternalType = "";
        logger.debug(" The cql3 type received is " + cql3DataType);

        if ( cql3DataType.equalsIgnoreCase("text") ) {
            cassInternalType = "org.apache.cassandra.db.marshal.UTF8Type";
        } else if ( cql3DataType.equalsIgnoreCase("int") ) {
            cassInternalType = "org.apache.cassandra.db.marshal.Int32Type";
        } else if ( cql3DataType.equalsIgnoreCase("map <int, text>") ) {
            cassInternalType = "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)";
        }
        return cassInternalType;
    }


    /**
     * It returns an equivalent Java data type from CQL3 data type
     **/
    public static String getJavaDataTypeFromCQL3DataType(String cql3DataType) {
        String javaDataType = "";
        logger.debug(" The cassandra type received is " + cql3DataType);

        if ( cql3DataType.equalsIgnoreCase("text") ) {
            javaDataType = "String";
        } else if ( cql3DataType.equalsIgnoreCase("int") ) {
            javaDataType = "Integer";
        }
        return javaDataType;
    }


    /**
     * It returns the stack trace as a String.
     **/
    public static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        return exceptionAsString;
    }


    /**
     * It returns an array separating keyspace and table name.
     **/
    public static String[] getKeyspaceAndTableNameInAnArray(String completeName) {
        String[] arr = new String[2];
        if ( completeName != null && !completeName.equalsIgnoreCase("") && completeName.contains(".") ) {
            arr = completeName.split("\\.");
        }
        return arr;
    }

    /**
     * A generic parser for where expressions which returns a list of all the expressions present.
     **/
    public static List<Expression> parseWhereExpression(Expression whereExpression) {

        if ( whereExpression != null && (!(whereExpression instanceof AndExpression) && !(whereExpression instanceof OrExpression)) ) {
            List<Expression> temp = new ArrayList<>();
            temp.add(whereExpression);
            return temp;
        }

        List<Expression> whereExpressions = new ArrayList<>();
        if ( whereExpression instanceof AndExpression ) {
            AndExpression andExpression = (AndExpression) whereExpression;

            if ( andExpression.getLeftExpression() instanceof AndExpression ||
                    andExpression.getLeftExpression() instanceof OrExpression ) {
                for ( Expression exp : parseWhereExpression(andExpression.getLeftExpression()) ) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(andExpression.getLeftExpression());
            }

            if ( andExpression.getRightExpression() instanceof AndExpression ||
                    andExpression.getRightExpression() instanceof OrExpression ) {
                for ( Expression exp : parseWhereExpression(andExpression.getRightExpression()) ) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(andExpression.getRightExpression());
            }

        } else if ( whereExpression instanceof OrExpression ) {
            OrExpression orExpression = (OrExpression) whereExpression;

            if ( orExpression.getLeftExpression() instanceof OrExpression ||
                    orExpression.getLeftExpression() instanceof AndExpression ) {
                for ( Expression exp : parseWhereExpression(orExpression.getLeftExpression()) ) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(orExpression.getLeftExpression());
            }

            if ( orExpression.getRightExpression() instanceof OrExpression ||
                    orExpression.getRightExpression() instanceof AndExpression ) {
                for ( Expression exp : parseWhereExpression(orExpression.getRightExpression()) ) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(orExpression.getRightExpression());
            }

        }

        return whereExpressions;

    }


    /**
     * Removes junk comma and space and joins values part to an insert query
     **/

    public static String joinInsertAndValues(StringBuffer insertStatement, StringBuffer valuesPart) {

        if ( valuesPart.lastIndexOf(", ") == valuesPart.length() - 2 ) {
            valuesPart.delete(valuesPart.length() - 2, valuesPart.length());
        }

        if ( insertStatement.lastIndexOf(", ") == insertStatement.length() - 2 ) {
            insertStatement.delete(insertStatement.length() - 2, insertStatement.length());
        }

        return insertStatement.toString();
    }


    /**
     * Removes comma and a space together if found at the end of the StringBuffer
     **/

    public static StringBuffer removesCommaSpace(StringBuffer stringBuffer) {
        if ( stringBuffer.lastIndexOf(", ") == stringBuffer.length() - 2 ) {
            stringBuffer.delete(stringBuffer.length() - 2, stringBuffer.length());
        }

        return stringBuffer;
    }


    /**
     * Returns true if a column is present in another table with the same name
     **/

    public static boolean checkPresenceOfColumnInDifferentTable(String baseTable, String columnName,
                                                                Map<String, Map<String, ColumnDefinition>> baseTablesDefinitionsMap) {
        for ( Map.Entry<String, Map<String, ColumnDefinition>> table : baseTablesDefinitionsMap.entrySet() ) {
            if ( table.getKey().equalsIgnoreCase(baseTable) ) {
                continue;
            }
            for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : table.getValue().entrySet() ) {
                if ( columnDefinitionEntry.getValue().name.toString().equalsIgnoreCase(columnName) ) {
                    logger.debug(" Column {} exists in other table as well!!", columnName);
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Returns the row with existing record if there exists else returns null
     **/
    public static Row getExistingRecordIfExists(PrimaryKey primaryKey, Table table) {
        Statement existingRecordQuery = null;
        // Checking if there is an already existing entry for the join key received
        if ( primaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

            existingRecordQuery = QueryBuilder.select().all().from(table.getKeySpace(),
                    table.getName()).where(QueryBuilder
                    .eq(primaryKey.getColumnName(),
                            Integer.parseInt(primaryKey.getColumnValueInString())));
        } else if ( primaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            existingRecordQuery = QueryBuilder.select().all().from(table.getKeySpace(),
                    table.getName()).where(QueryBuilder
                    .eq(primaryKey.getColumnName(),
                            primaryKey.getColumnValueInString()));
        }

        logger.debug("#### Existing Record Query :: " + existingRecordQuery);

        List<Row> existingRows = CassandraClientUtilities.commandExecution("localhost", existingRecordQuery);


        if ( existingRows.size() > 0 ) {
            logger.debug("#### Existing record in view table{} :: {}", table.getKeySpace() +
                    "." + table.getName(), existingRows.get(0));
            return existingRows.get(0);
        }

        return null;
    }


    public static String checkForChangeInAggregationKeyInDeltaView(List<String> aggregationKeyData, Row deltaTableRecord) {
        logger.debug("#### Checking --- aggregationKeyData :: " + aggregationKeyData);
        logger.debug("#### Checking --- deltaTableRecord :: " + deltaTableRecord);
        String result = "";
        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("Integer") ) {
            if ( deltaTableRecord.getInt(aggregationKeyData.get(0) + DeltaViewTrigger.CURRENT) ==
                    deltaTableRecord.getInt(aggregationKeyData.get(0) + DeltaViewTrigger.LAST) ) {
                result = "unchanged";
            } else {
                if ( deltaTableRecord.getInt(aggregationKeyData.get(0) + DeltaViewTrigger.LAST) == 0 ) {
                    result = "new";
                } else {
                    result = "changed";
                }
            }
        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("String") ) {
            if ( deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.CURRENT).equalsIgnoreCase(
                    deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.LAST)) ) {
                result = "unchanged";
            } else {
                if ( deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.LAST) == null
                        || deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.LAST).isEmpty() ) {
                    result = "new";
                } else {
                    result = "changed";
                }
            }
        }

        logger.debug("#### Result for checkForChangeInAggregationKey :: " + result);
        return result;
    }


    public static List<Expression> getParticularWhereExpressionBasedOnBaseTableOperation(String sqlString,
                                                                                         String baseTableName) {
        List<Expression> finalExpList = new ArrayList<>();
        net.sf.jsqlparser.statement.Statement stmt = null;
        try {
            stmt = CCJSqlParserUtil.parse(sqlString);
        } catch ( JSQLParserException e ) {
            logger.debug("Error!!!" + getStackTrace(e));
        }
        Select select = (Select) stmt;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        List<Expression> expressions = ViewMaintenanceUtilities.parseWhereExpression(plainSelect.getWhere());
        for ( Expression expression : expressions ) {
            Column column = ViewMaintenanceUtilities.getColumnObject(expression);
            if ( column.getTable().getName().equalsIgnoreCase(baseTableName) ) {
                finalExpList.add(expression);
            }
        }
        return finalExpList;
    }

    /**
     * It gets type of logical operator present.
     * Either AND or OR is returned
     **/
    public static String getAndOrBasedOnWhereExpression(String sqlString) {
        String finalResult = "";
        net.sf.jsqlparser.statement.Statement stmt = null;
        try {
            stmt = CCJSqlParserUtil.parse(sqlString);

        } catch ( JSQLParserException e ) {
            logger.debug("Error!!!" + getStackTrace(e));
        }

        Select select = (Select) stmt;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        if ( plainSelect.getWhere() instanceof AndExpression ) {
            finalResult = "and";
        } else if ( plainSelect.getWhere() instanceof OrExpression ) {
            finalResult = "or";
        }
        return finalResult;
    }


    public static Column getColumnObject(Expression expression) {
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


    public static boolean checkExpression(Expression whereExpression, Map<String, List<String>> columnMap) {

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


    public static Map<String, List<String>> getColumnMapFromDataJson(LinkedTreeMap dataJsonMap, Table table) {
        Map<String, List<String>> columnMap = new HashMap<>();
        Map<String, ColumnDefinition> tableDesc = ViewMaintenanceUtilities.getTableDefinitition(table.getKeySpace(), table.getName());
        Set keySet = dataJsonMap.keySet();
        Iterator dataIter = keySet.iterator();
        String primaryKey = null;
        while ( dataIter.hasNext() ) {
            String tempDataKey = (String) dataIter.next();
            logger.debug("Key: " + tempDataKey);
            logger.debug("Value: " + dataJsonMap.get(tempDataKey));
            for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : tableDesc.entrySet() ) {
                ColumnDefinition columnDefinition = columnDefinitionEntry.getValue();
                if ( tempDataKey.equalsIgnoreCase(columnDefinition.name.toString()) ) {
                    List<String> tempList = new ArrayList<>(); // Format of storing: internalcassandra type, value, isPrimaryKey

                    tempList.add(columnDefinition.type.toString());

                    if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                            columnDefinition.type.toString()).equalsIgnoreCase("String") ) {
                        tempList.add(((String) dataJsonMap.get(tempDataKey)).replaceAll("'", ""));
                    } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                            columnDefinition.type.toString()).equalsIgnoreCase("Integer") ) {
                        tempList.add((String) dataJsonMap.get(tempDataKey));
                    }
                    tempList.add(columnDefinition.isPartitionKey() ? "true" : "false");

                    columnMap.put(tempDataKey, tempList);
                }
            }

        }

        return columnMap;
    }


    public static LinkedTreeMap createDataJsonForOldValue(LinkedTreeMap dataJsonMap, List<String> oldColData) {
        // Format: oldColData: ColName, Cass Type, Value
        LinkedTreeMap finalDataJsonMap = null;
        Set keySet = dataJsonMap.keySet();
        Iterator dataIter = keySet.iterator();
        while ( dataIter.hasNext() ) {
            String tempDataKey = (String) dataIter.next();
            if ( tempDataKey.equalsIgnoreCase(oldColData.get(0)) ) {
                dataJsonMap.put(tempDataKey, oldColData.get(2));
            }
        }
        return dataJsonMap;
    }


    public static boolean didOldValueSatisfyWhereClause(Table viewConfig, TriggerRequest triggerRequest,
                                                        List<String> userData, Row deltaTableRecord,
                                                        Table inputWhereTable) {
        List<Expression> expressions = ViewMaintenanceUtilities
                .getParticularWhereExpressionBasedOnBaseTableOperation(viewConfig.getSqlString(),
                        triggerRequest.getBaseTableName());
        logger.debug("### Getting pertinent expressions :: " + expressions);

        String logicalOperatorInvovled = ViewMaintenanceUtilities.getAndOrBasedOnWhereExpression(
                viewConfig.getSqlString());
        logger.debug("### Getting the logical operator involved :: " + logicalOperatorInvovled);

        LinkedTreeMap dataJson = triggerRequest.getDataJson();

        List<String> oldValData = new ArrayList<>();
        oldValData.add(userData.get(2));
        oldValData.add(ViewMaintenanceUtilities.getCassInternalDataTypeFromCQL3DataType("int"));
        oldValData.add(deltaTableRecord.getInt(userData.get(2) + DeltaViewTrigger.LAST) + "");

        logger.debug("### Old Val Data :: " + oldValData);
        LinkedTreeMap oldDataJsonMap = ViewMaintenanceUtilities.createDataJsonForOldValue(dataJson,
                oldValData);

        logger.debug("### oldDataJsonMap :: " + oldDataJsonMap);


        Map<String, List<String>> columnMapWhereTable = ViewMaintenanceUtilities.getColumnMapFromDataJson(
                oldDataJsonMap, inputWhereTable);

        logger.debug("### Get columnMapWhereTable for old target column :: " + columnMapWhereTable);
        boolean didOldValueSatisfyWhereClause = true;

        if ( logicalOperatorInvovled.equalsIgnoreCase("and") ) {
            for ( Expression expression : expressions ) {

                logger.debug("#### Checking for expression :: " + expression);
                if ( !ViewMaintenanceUtilities.checkExpression(expression, columnMapWhereTable) ) {
                    didOldValueSatisfyWhereClause = false;
                    break;
                }
            }

        } else if ( logicalOperatorInvovled.equalsIgnoreCase("or") ) {
            // TODO: Not yet implemented
        } else {
            logger.debug("### Single expression case!! expression = " + expressions.get(0));
            logger.debug("### ColumnMapWhereTable(with old target value) :: " + columnMapWhereTable);
            if ( !ViewMaintenanceUtilities.checkExpression(expressions.get(0), columnMapWhereTable) ) {
                didOldValueSatisfyWhereClause = false;
            }
        }


        return didOldValueSatisfyWhereClause;

    }

    public static Table getConcernedWhereTableFromWhereTablesList(TriggerRequest triggerRequest, List<Table> whereTables) {
        for ( Table tempWhereTable: whereTables) {
            if (tempWhereTable.getName().matches("vt(\\d+)_where_" + triggerRequest.getBaseTableName())) {
                return tempWhereTable;
            }
        }

        return null;
    }

}
