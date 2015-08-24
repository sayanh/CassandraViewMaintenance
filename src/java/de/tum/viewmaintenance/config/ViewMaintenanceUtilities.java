package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
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
        } else if ( internalDataType.equalsIgnoreCase("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)")) {
            cql3Type = "map <int, text>";
        }
        return cql3Type;
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
     *
     **/

    public static boolean checkPresenceOfColumnInDifferentTable(String baseTable, String columnName,
                                                          Map<String, Map<String, ColumnDefinition>> baseTablesDefinitionsMap) {
        for (Map.Entry<String, Map<String, ColumnDefinition>> table: baseTablesDefinitionsMap.entrySet()){
            if (table.getKey().equalsIgnoreCase(baseTable)) {
                continue;
            }
            for (Map.Entry<String, ColumnDefinition> columnDefinitionEntry: table.getValue().entrySet()) {
                if (columnDefinitionEntry.getValue().name.toString().equalsIgnoreCase(columnName)){
                    logger.debug(" Column {} exists in other table as well!!", columnName);
                    return true;
                }
            }
        }
        return false;
    }


}
