package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.thrift.Cassandra;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 8/14/15.
 */
public class PreAggOperation extends GenericOperation {

    private static final Logger logger = LoggerFactory.getLogger(PreAggOperation.class);
    private static final List<String> AVAILABLE_FUNCS = Arrays.asList("sum", "count", "min", "max");
    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Table viewConfig;

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

        if ( inputViewTables == null ) {
            // Case: No where, join and having clause
            String baseTableInvolved = viewConfig.getRefBaseTable();
            String baseTableInvolvedArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(baseTableInvolved);

            Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities.getTableDefinitition(baseTableInvolvedArr[0],
                    baseTableInvolvedArr[1]);
            logger.debug("#### Base table description :: " + baseTableDesc);
            List<String> userData = new ArrayList<>(); // For target column: FunctionName, Value, TargetColumnName
            List<String> aggregationKeyData = new ArrayList<>(); // For curr aggregation key: columnName, Cass Type
            PrimaryKey preAggTablePK = null;
            PrimaryKey baseTablePK = null;
            LinkedTreeMap dataJson = triggerRequest.getDataJson();

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
            String statusEntryColAggKey = ViewMaintenanceUtilities.checkForChangeInAggregationKeyInDeltaView(
                    aggregationKeyData, deltaTableRecord);

            // check for the change in aggregation key over the time
            if ( statusEntryColAggKey.equalsIgnoreCase("changed") ) {
                // Aggregation key got changed
                Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                        operationViewTables.get(0));

                intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);

                // Delete(actually update) the amount for the old aggregation key
                if ( userData.get(0).equalsIgnoreCase("sum") ) {
                    deleteFromSumPreAggView(getOldAggregationKeyAsPrimaryKey(aggregationKeyData,
                            baseTableInvolvedArr[1]), userData.get(0) + "_" + userData.get(2));
                } else if ( userData.get(0).equalsIgnoreCase("count") ) {
                    deleteFromCountPreAggView(getOldAggregationKeyAsPrimaryKey(aggregationKeyData,
                            baseTableInvolvedArr[1]), userData.get(0) + "_" + userData.get(2));
                } else if ( userData.get(0).equalsIgnoreCase("max") ) {

                } else if ( userData.get(0).equalsIgnoreCase("min") ) {

                }


            } else {
                // Aggregation key remained same or new entry with old value as null
                Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                        operationViewTables.get(0));
                if ( statusEntryColAggKey.equalsIgnoreCase("new") ) {
                    intelligentEntryPreAggViewTable(existingRecordPreAggTable, preAggTablePK, userData);
                } else if ( statusEntryColAggKey.equalsIgnoreCase("unchanged") ) {
                    String statusTargetCol = checkForChangeInTargetColValue(userData);
                    if ( userData.get(0).equalsIgnoreCase("sum") && statusTargetCol.equalsIgnoreCase("changed") ) {
                        updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
                    } else if ( userData.get(0).equalsIgnoreCase("count") ) {
//                        updateCountPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
                        // count does not undergo any change
                    } else if ( userData.get(0).equalsIgnoreCase("max") ) {
//                        updateMaxPreAggView();
                    } else if ( userData.get(0).equalsIgnoreCase("min") ) {

                    }
                }


                if ( existingRecordPreAggTable != null ) {
                    logger.debug("### Existing record in the preagg table :: " + existingRecordPreAggTable);
                    if ( userData.get(0).equalsIgnoreCase("sum") ) {
                        updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
                    }
                }

                // There is no change in count view


            }


        }
        return true;

    }

    private void intelligentEntryPreAggViewTable(Row existingRecordPreAggTable, PrimaryKey preAggTablePK, List<String> userData) {
        if ( existingRecordPreAggTable != null ) {

            logger.debug("### Existing record in preAggregateView :: " + existingRecordPreAggTable);
            // Update Agg function column for preagg view table
            if ( userData.get(0).equalsIgnoreCase("sum") ) {
                updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
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

        if (statusEntryColAggKey.equalsIgnoreCase("unchanged")) {
            // If there is no key change then update involves subtraction of the old value and addition of the new
            newValue = (existingVal - oldValue) + Integer.parseInt(userData.get(1));
        } else {
            // If there is a key change or new addition then update involves just addition
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

}
