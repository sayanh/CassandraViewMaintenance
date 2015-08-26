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

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public static PreAggOperation getInstance(Row deltaTableRecord, List<Table> inputViewTables,
                                              List<Table> operationViewTables) {
        PreAggOperation preAggOperation = new PreAggOperation();
        preAggOperation.setDeltaTableRecord(deltaTableRecord);
        preAggOperation.setInputViewTables(inputViewTables);
        preAggOperation.setOperationViewTables(operationViewTables);
        return preAggOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        logger.debug("##### Entering insert trigger for PreAggregate Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure {}", this.inputViewTables);
        logger.debug("##### Trigger request :: " + triggerRequest);

        if ( inputViewTables == null ) {
            // Case: No where, join and having clause
            String baseTableInvolved = viewConfig.getRefBaseTable();
            String baseTableInvolvedArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(baseTableInvolved);
            Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities.getTableDefinitition(baseTableInvolvedArr[0],
                    baseTableInvolvedArr[1]);
            List<String> userData = new ArrayList<>(); // For target column: FunctionName, Value, TargetColumnName
            List<String> aggregationKeyData = new ArrayList<>(); // For aggregation key: columnName, Cass Type
            PrimaryKey preAggTablePK = null;
            PrimaryKey baseTablePK = null;
            LinkedTreeMap dataJson = triggerRequest.getDataJson();

            for ( Column column : operationViewTables.get(0).getColumns() ) {
                String derivedColumnName = column.getName().substring(column.getName().indexOf("_") + 1);
                String prefixForColName = column.getName().substring(0, column.getName().indexOf("_"));
                if ( AVAILABLE_FUNCS.contains(prefixForColName) ) {
                    userData.add(prefixForColName);
                    userData.add(((String) dataJson.get(derivedColumnName)).replaceAll("'",
                            ""));
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
                                    .getValue().type.toString(), ((String) dataJson.get(derivedColumnName)).replaceAll("'", ""));
                        }
                    }
                }
            }


            // check for the change in aggregation key over the time
            if ( checkForChangeInAggregationKey(aggregationKeyData) ) {
                // Aggregation key got changed
                Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                        operationViewTables.get(0));

                if ( existingRecordPreAggTable != null ) {

                    logger.debug("### Existing record in preAggregateView :: " + existingRecordPreAggTable);
                    // Update Agg function column for preagg view table
                    if ( userData.get(2).equalsIgnoreCase("sum") ) {

                        updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
                    } else if ( userData.get(2).equalsIgnoreCase("count") ) {

                    } else if ( userData.get(2).equalsIgnoreCase("max") ) {

                    } else if ( userData.get(2).equalsIgnoreCase("min") ) {

                    }
                } else {
                    // Insert into Agg function column for preagg view table
                    if ( userData.get(2).equalsIgnoreCase("sum") ) {
                        insertIntoSumPreAggView(preAggTablePK, userData);
                    } else if ( userData.get(2).equalsIgnoreCase("count") ) {

                    } else if ( userData.get(2).equalsIgnoreCase("max") ) {

                    } else if ( userData.get(2).equalsIgnoreCase("min") ) {

                    }
                }

                // Delete(actually update) the amount for the old aggregation key
                if ( userData.get(2).equalsIgnoreCase("sum") ) {
                    deleteFromSumPreAggView(getOldAggregationKeyAsPrimaryKey(aggregationKeyData), userData.get(2));
                } else if ( userData.get(2).equalsIgnoreCase("count") ) {

                } else if ( userData.get(2).equalsIgnoreCase("max") ) {

                } else if ( userData.get(2).equalsIgnoreCase("min") ) {

                }


            } else {
                // Aggregation key remained same
                Row existingRecordPreAggTable = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggTablePK,
                        operationViewTables.get(0));
                updateSumPreAggView(preAggTablePK, existingRecordPreAggTable, userData);
            }


        }
        return true;
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
        return "PreAggOperation{" +
                "\n deltaTableRecord=" + deltaTableRecord +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }


    private void updateSumPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable, List<String> userData) {
        int existingVal = existingRecordPreAggTable.getInt(userData.get(0) + "_" + userData.get(2));
        int newValue = existingVal + Integer.parseInt(userData.get(1));
        String modifiedColumnName = "sum_" + userData.get(2);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with();
        Statement updateSumQuery = null;
        if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            updateSumQuery = assignments.and(QueryBuilder.add(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), Integer.parseInt(preAggTablePK.getColumnValueInString())));

        } else if ( preAggTablePK.getColumnJavaType().equalsIgnoreCase("String") ) {
            updateSumQuery = assignments.and(QueryBuilder.add(modifiedColumnName, newValue)).where(
                    QueryBuilder.eq(preAggTablePK.getColumnName(), preAggTablePK.getColumnValueInString()));
        }

        logger.debug("### UpdateSumQuery :: " + updateSumQuery);

        CassandraClientUtilities.commandExecution("localhost", updateSumQuery);

    }

    private void updateCountPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable, List<String> userData) {

    }

    private void updateMaxPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable) {

    }

    private void updateMinPreAggView(PrimaryKey preAggTablePK, Row existingRecordPreAggTable) {

    }

    private void insertIntoSumPreAggView(PrimaryKey preAggTablePK, List<String> userData) {

        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();

        columnNames.add(preAggTablePK.getColumnName());
        if (preAggTablePK.getColumnJavaType().equalsIgnoreCase("Integer")) {
            objects.add(Integer.parseInt(preAggTablePK.getColumnValueInString()));
        } else if (preAggTablePK.getColumnJavaType().equalsIgnoreCase("String")) {
            objects.add(preAggTablePK.getColumnValueInString());
        }


        columnNames.add(userData.get(2));
        objects.add(Integer.parseInt(userData.get(1)));

        Statement insertQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).values(columnNames.toArray(new String[columnNames.size()]),
                objects.toArray());


        logger.debug("### Insert query for Sum into pre agg view table :: " + insertQuery);

        CassandraClientUtilities.commandExecution("localhost", insertQuery);

    }

    private void insertIntoCountPreAggView(PrimaryKey preAggTablePK) {

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
        int oldAggValue = existingRecordOldAggKey.getInt("sum_" + oldAggregateKey.getColumnName());
        int subtractionAmount = deltaTableRecord.getInt(targetColName + DeltaViewTrigger.LAST);
        Update.Assignments assignments = QueryBuilder.update(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).with(QueryBuilder.add(oldAggregateKey.getColumnName(),
                (oldAggValue - subtractionAmount)));
        if (oldAggregateKey.getColumnJavaType().equalsIgnoreCase("Integer")) {
            updateQuery = assignments.where(QueryBuilder.eq(oldAggregateKey.getColumnName(),
                    Integer.parseInt(oldAggregateKey.getColumnValueInString())));
        } else if (oldAggregateKey.getColumnJavaType().equalsIgnoreCase("String")) {
            updateQuery = assignments.where(QueryBuilder.eq(oldAggregateKey.getColumnName(),
                    oldAggregateKey.getColumnValueInString()));
        }

        logger.debug("### Delete query for sum in preagg View :: " + updateQuery);
        CassandraClientUtilities.commandExecution("localhost", updateQuery);
    }


    private boolean checkForChangeInAggregationKey(List<String> aggregationKeyData) {
        boolean isChanged = false;
        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("Integer") ) {
            if ( deltaTableRecord.getInt(aggregationKeyData.get(0) + DeltaViewTrigger.CURRENT) ==
                    deltaTableRecord.getInt(aggregationKeyData.get(0) + DeltaViewTrigger.LAST) ) {
                isChanged = true;
            } else {
                isChanged = false;
            }
        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("String") ) {
            if ( deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.CURRENT).equalsIgnoreCase(
                    deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.LAST)) ) {
                isChanged = true;
            } else {
                isChanged = false;
            }
        }
        return isChanged;
    }

    private PrimaryKey getOldAggregationKeyAsPrimaryKey(List<String> aggregationKeyData) {
        PrimaryKey oldAggKey = null;
        if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("Integer") ) {
            oldAggKey = new PrimaryKey(aggregationKeyData.get(0), aggregationKeyData.get(1),
                    deltaTableRecord.getInt(aggregationKeyData.get(0) + DeltaViewTrigger.LAST) + "");
        } else if ( ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggregationKeyData.get(1))
                .equalsIgnoreCase("String") ) {
            oldAggKey = new PrimaryKey(aggregationKeyData.get(0), aggregationKeyData.get(1),
                    deltaTableRecord.getString(aggregationKeyData.get(0) + DeltaViewTrigger.LAST));
        }
        return oldAggKey;
    }

}
