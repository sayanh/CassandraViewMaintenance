package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 8/14/15.
 */
public class AggOperation extends GenericOperation {

    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Expression havingExpression;
    private static final Logger logger = LoggerFactory.getLogger(AggOperation.class);
    private static final List<String> AVAILABLE_FUNCS = Arrays.asList("sum", "count", "min", "max");

    public List<Table> getInputViewTables() {
        return inputViewTables;
    }

    public void setInputViewTables(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public List<Table> getOperationViewTables() {
        return operationViewTables;
    }

    public void setOperationViewTables(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    public Expression getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(Expression havingExpression) {
        this.havingExpression = havingExpression;
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

    public static AggOperation getInstance(List<Table> inputViewTables,
                                           List<Table> operationViewTables) {
        AggOperation aggOperation = new AggOperation();
        aggOperation.setInputViewTable(inputViewTables);
        aggOperation.setOperationViewTable(operationViewTables);
        return aggOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {

        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Entering insert trigger for Aggregate Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure {}", this.inputViewTables);
        logger.debug("##### Trigger request :: " + triggerRequest);

        Table preAggTable = inputViewTables.get(0);
        String functionName = "";
        String targetColName = "";
        String colAggKey = "";
        Map<String, ColumnDefinition> preAggDesc = ViewMaintenanceUtilities.getTableDefinitition(preAggTable.getKeySpace(),
                preAggTable.getName());
        Map<String, List<String>> userData = new HashMap<>();
        // Column Name Mapped to -> internalCassandraType, value, isPrimaryKey

        PrimaryKey preAggPKey = null;

        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();


        // Preparing the columnMap with current data


        logger.debug("#### Description of pre agg desc :: " + preAggDesc);

        for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : preAggDesc.entrySet() ) {
            String derivedPrefix = columnDefinitionEntry.getKey().substring(0, columnDefinitionEntry.getKey()
                    .indexOf("_"));
            String derivedColumnName = columnDefinitionEntry.getKey().substring(columnDefinitionEntry.getKey()
                    .indexOf("_") + 1);
            if ( AVAILABLE_FUNCS.contains(derivedPrefix) ) {
                functionName = derivedPrefix;
                targetColName = derivedColumnName;
                logger.debug("#### Checking -- functionName: {} | targetColName: {}", functionName, targetColName);
            }
            Iterator dataIter = keySet.iterator();
            while ( dataIter.hasNext() ) {
                String tempDataKey = (String) dataIter.next();
//                logger.debug("Key: " + tempDataKey);
//                logger.debug("Value: " + dataJson.get(tempDataKey));

                if ( columnDefinitionEntry.getValue().isPartitionKey() && tempDataKey.equalsIgnoreCase(derivedColumnName) ) {
                    List<String> tempList = new ArrayList<>(); //Format: internalCassandraType, value, isPrimaryKey
                    tempList.add(columnDefinitionEntry.getValue().type.toString());
                    tempList.add(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                    tempList.add("true");
                    colAggKey = derivedColumnName;
                    userData.put(columnDefinitionEntry.getKey(), tempList);

                    preAggPKey = new PrimaryKey(columnDefinitionEntry.getKey(), columnDefinitionEntry.getValue().type
                            .toString(), ((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                    logger.debug("### Primary key for pre agg table :: " + preAggPKey);
                }
            }

            if ( !columnDefinitionEntry.getValue().isPartitionKey() ) {
                List<String> tempList = new ArrayList<>(); //Format: internalCassandraType, value, isPrimaryKey
                tempList.add(columnDefinitionEntry.getValue().type.toString());
                tempList.add("");
                tempList.add("false");
                userData.put(columnDefinitionEntry.getKey(), tempList);
            }
        }
        logger.debug("### Tentative userdata :: " + userData);

        Row existingRecordPreAgg = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggPKey, preAggTable);
        Row existingRecordAggView = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggPKey,
                operationViewTables.get(0));


        // Check if the record exists in pre agg view
        if ( existingRecordPreAgg == null ) {
            logger.debug("#### Record does not exist in pre agg view table...");

            // Situation:
            // Either the record is already there in agg table or not there at all
            // If the record is already there then it needs to be removed from the agg view table.
            // If the record is not there in there in the agg view table nothing needs to be done.

            // Check if the record exists in agg view
            if ( existingRecordAggView != null ) {
                // Delete the record from the agg view table
                deleteRecordFromAggViewTable(preAggPKey);
            }


        } else {
            logger.debug("#### Record exists in pre agg view table!! ");
            List<String> targetColData = userData.get(functionName + "_" + targetColName);
            targetColData.set(1, existingRecordPreAgg.getInt(functionName + "_" + targetColName) + "");
            userData.put(functionName + "_" + targetColName, targetColData);

            logger.debug("#### User data with values :: " + userData);

//            Function function = (Function) ((GreaterThan) havingExpression).getLeftExpression();
//            Expression expression = function.getParameters().getExpressions().get(0)

            logger.debug("### Having expression :: " + havingExpression);

            // Checking if the record satisfies having clause
            if ( ViewMaintenanceUtilities.checkHavingExpression(havingExpression, targetColData) ) {
                // Insert the record into the agg view table
                insertIntoAggViewTable(userData);
            } else {
                // Delete the record from the agg view table
                if ( existingRecordPreAgg != null ) {

                    deleteRecordFromAggViewTable(preAggPKey);
                }
            }


        }

        PrimaryKey oldPreAggPKey = null;
        List<String> aggregationKeyData = new ArrayList<>();
        aggregationKeyData.add(colAggKey); // Aggregation key column name
        aggregationKeyData.add(userData.get(preAggPKey.getColumnName()).get(0)); // Aggregation column type

        logger.debug("### Aggregation key data :: " + aggregationKeyData);
        String statusChangeInColAggKey = ViewMaintenanceUtilities.checkForChangeInAggregationKeyInDeltaView(aggregationKeyData,
                deltaTableRecord);

        if ( statusChangeInColAggKey.equalsIgnoreCase("changed") ) {


            // If there is a change in the colAggKey then change the details for the old agg key
            if ( preAggPKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                String oldAggKeyValStr = deltaTableRecord.getInt(colAggKey + DeltaViewTrigger.LAST) + "";
                oldPreAggPKey = new PrimaryKey(preAggPKey.getColumnName(), preAggPKey.getColumnInternalCassType(),
                        oldAggKeyValStr);
            } else if ( preAggPKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                String oldAggKeyValStr = deltaTableRecord.getString(colAggKey + DeltaViewTrigger.LAST);
                oldPreAggPKey = new PrimaryKey(preAggPKey.getColumnName(), preAggPKey.getColumnInternalCassType(),
                        oldAggKeyValStr);
            }

            Row existingOldAggKeyVal = ViewMaintenanceUtilities.getExistingRecordIfExists(oldPreAggPKey, operationViewTables.get(0));
            logger.debug("### Existing value for old Agg key in Agg View : " + existingOldAggKeyVal);
            Row existingOldPreAggKeyVal = ViewMaintenanceUtilities.getExistingRecordIfExists(oldPreAggPKey,
                    preAggTable);
            logger.debug("### Existing value for old Agg key in PreAgg View : " + existingOldPreAggKeyVal);

            // Checking whether the old agg key exists in agg view table
            if ( existingOldPreAggKeyVal != null ) {
                Map<String, List<String>> userDataWithOldAggKey = userData;
                List<String> targetOldAggData = userData.get(oldPreAggPKey.getColumnName());
                if ( oldPreAggPKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {

                    targetOldAggData.set(1, existingOldPreAggKeyVal.getInt(oldPreAggPKey.getColumnName()) + "");
                } else if ( oldPreAggPKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                    targetOldAggData.set(1, existingOldPreAggKeyVal.getString(oldPreAggPKey.getColumnName()));
                }
                logger.debug("#### Target old col agg key :: " + targetOldAggData);

                userDataWithOldAggKey.put(oldPreAggPKey.getColumnName(), targetOldAggData);

                List<String> functionColForOldAggKey = userDataWithOldAggKey.get(functionName + "_" + targetColName);
                functionColForOldAggKey.set(1, existingOldPreAggKeyVal.getInt(functionName + "_" + targetColName) + "");
                userDataWithOldAggKey.put(functionName + "_" + targetColName, functionColForOldAggKey);

                logger.debug("#### functionColForOldAggKey : " + functionColForOldAggKey);
                logger.debug("#### userDataWithOldAggKey : " + userDataWithOldAggKey);

                if ( ViewMaintenanceUtilities.checkHavingExpression(havingExpression, targetOldAggData) ) {
                    logger.debug("### Inserting the old agg key as it satisfies having clause!!");
                    insertIntoAggViewTable(userDataWithOldAggKey);
                } else {
                    if ( existingOldAggKeyVal != null ) {
                        logger.debug("### Deleting the old col agg key, primary : " + oldPreAggPKey);

                        deleteRecordFromAggViewTable(oldPreAggPKey);
                    }
                }
            }

        }


        return true;
    }


    private void insertIntoAggViewTable(Map<String, List<String>> userData) {

        List<String> colNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();

        for ( Map.Entry<String, List<String>> entry : userData.entrySet() ) {
            String javaDataType = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(entry.getValue().get(0));
            colNames.add(entry.getKey());
            if ( javaDataType.equalsIgnoreCase("Integer") ) {
                objects.add(Integer.parseInt(entry.getValue().get(1)));
            } else if ( javaDataType.equalsIgnoreCase("String") ) {
                objects.add(entry.getValue().get(1));
            }
        }

        Statement insertIntoAggViewQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).values(colNames.toArray(new String[colNames.size()]),
                objects.toArray());

        logger.debug("### Insert query into Agg View :: " + insertIntoAggViewQuery);

        CassandraClientUtilities.commandExecution("localhost", insertIntoAggViewQuery);

    }


    private void deleteRecordFromAggViewTable(PrimaryKey primaryKey) {


        Clause clause = null;

        if ( primaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            clause = QueryBuilder.eq(primaryKey.getColumnName(), Integer.parseInt(primaryKey.getColumnValueInString()));
        } else if ( primaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            clause = QueryBuilder.eq(primaryKey.getColumnName(), primaryKey.getColumnValueInString());
        }

        Statement deleteQueryFromAggView = QueryBuilder.delete().from(operationViewTables.get(0).getKeySpace(),
                operationViewTables.get(0).getName()).where(clause);

        logger.debug("### Delete query from agg view table " + deleteQueryFromAggView);

        CassandraClientUtilities.commandExecution("localhost", deleteQueryFromAggView);

    }

    @Override
    public boolean updateTrigger(TriggerRequest triggerRequest) {
        return false;
    }

    @Override
    public boolean deleteTrigger(TriggerRequest triggerRequest) {
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Entering delete trigger for Aggregate Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        logger.debug("##### Input tables structure {}", this.inputViewTables);
        logger.debug("##### Trigger request :: " + triggerRequest);

        Table preAggTableConfig = inputViewTables.get(0);

        Map<String, ColumnDefinition> preAggTableDesc = ViewMaintenanceUtilities.getTableDefinitition(preAggTableConfig
                .getKeySpace(), preAggTableConfig.getName());

        Map<String, ColumnDefinition> aggTableDesc = ViewMaintenanceUtilities.getTableDefinitition(operationViewTables.get(0)
                .getKeySpace(), operationViewTables.get(0).getName());

        PrimaryKey preAggPrimaryKey = null;
        PrimaryKey aggTablePrimaryKey = null;
        String derivedColumnName = "";
        String cql3Type = "";

        for ( Map.Entry<String, ColumnDefinition> preAggColEntry : preAggTableDesc.entrySet() ) {
            if ( preAggColEntry.getValue().isPartitionKey() ) {

                derivedColumnName = preAggColEntry.getKey().substring(preAggColEntry.getKey().indexOf("_") + 1);
                cql3Type = ViewMaintenanceUtilities.getCQL3DataTypeFromCassandraInternalDataType(preAggColEntry
                        .getValue().type.toString());
                if ( cql3Type.equalsIgnoreCase("text") ) {
                    preAggPrimaryKey = new PrimaryKey(preAggColEntry.getKey(), preAggColEntry.getValue().type.toString(),
                            deltaTableRecord.getString(derivedColumnName + DeltaViewTrigger.CURRENT));

                    aggTablePrimaryKey = new PrimaryKey(preAggColEntry.getKey(), preAggColEntry.getValue().type.toString(),
                            deltaTableRecord.getString(derivedColumnName + DeltaViewTrigger.CURRENT));

                } else if ( cql3Type.equalsIgnoreCase("int") ) {
                    preAggPrimaryKey = new PrimaryKey(preAggColEntry.getKey(), preAggColEntry.getValue().type.toString(),
                            deltaTableRecord.getInt(derivedColumnName + DeltaViewTrigger.CURRENT) + "");
                    aggTablePrimaryKey = new PrimaryKey(preAggColEntry.getKey(), preAggColEntry.getValue().type.toString(),
                            deltaTableRecord.getString(derivedColumnName + DeltaViewTrigger.CURRENT));

                }

            }

            logger.debug("#### Pre agg table primary key :: " + preAggPrimaryKey);
            logger.debug("#### Agg table primary key :: " + aggTablePrimaryKey);

        }

        Row existingRecordInPreAgg = ViewMaintenanceUtilities.getExistingRecordIfExists(preAggPrimaryKey, preAggTableConfig);

        logger.debug("#### existingRecordInPreAgg :: " + existingRecordInPreAgg);
        Map<String, List<String>> userData = null;

        if (existingRecordInPreAgg != null) {
            userData = new HashMap<>();
            // Format::
            // Key: Name of the column
            // Value : CassandraInternalType, ValueInString, isPrimaryKeyForReverseJoinTable
            for ( Map.Entry<String, ColumnDefinition> aggTableEntry : aggTableDesc.entrySet() ) {
                List<String> tempList = new ArrayList<>();
                tempList.add(aggTableEntry.getValue().type.toString());

                String javaType = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(aggTableEntry.getValue().type.toString());

                if (javaType.equalsIgnoreCase("Integer")) {
                    tempList.add(existingRecordInPreAgg.getInt(aggTableEntry.getKey()) + "");
                } else if (javaType.equalsIgnoreCase("String")) {
                    tempList.add(existingRecordInPreAgg.getString(aggTableEntry.getKey())   );
                }

                if (aggTableEntry.getValue().isPartitionKey()) {
                    tempList.add("true");
                } else {
                    tempList.add("false");
                }

                userData.put(aggTableEntry.getKey(), tempList);
            }

            logger.debug("#### userData aggViewTable:: " + userData);
            insertIntoAggViewTable(userData);
        }




        return false;
    }

    @Override
    public String toString() {
        return "AggOperation{" +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                ",\n havingExpression=" + havingExpression +
                '}';
    }
}
