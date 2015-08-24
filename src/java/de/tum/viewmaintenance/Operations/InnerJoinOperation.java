package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 8/14/15.
 */
public class InnerJoinOperation extends GenericOperation {
    private static final Logger logger = LoggerFactory.getLogger(InnerJoinOperation.class);
    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Table viewConfig;

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
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

    public static InnerJoinOperation getInstance(Row deltaTableRecord, List<Table> inputViewTable,
                                                 List<Table> operationViewTable) {
        InnerJoinOperation innerJoinOperation = new InnerJoinOperation();
        innerJoinOperation.setDeltaTableRecord(deltaTableRecord);
        innerJoinOperation.setInputViewTable(inputViewTable);
        innerJoinOperation.setOperationViewTable(operationViewTable);
        return innerJoinOperation;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
        Statement reverseJoinRecordFetchQuery = null;
        logger.debug("##### Entering insert trigger for InnerJoin Operations!!! ");
        logger.debug("##### Received elements #####");
        logger.debug("##### Table structure involved: {}", this.operationViewTables);
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        Table reverseJoinTable = inputViewTables.get(0);
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        PrimaryKey innerJoinViewPrimaryKey = new PrimaryKey();
        List<String> baseTablesInvolved = viewConfig.getRefBaseTables();
        boolean viewMaintenanceEligibility = false;
        for ( String baseTableName : baseTablesInvolved ) {
            if ( baseTableName.equalsIgnoreCase(triggerRequest.getBaseTableKeySpace()
                    + "." + triggerRequest.getBaseTableName()) ) {
                viewMaintenanceEligibility = true;
            }
        }
        if ( !viewMaintenanceEligibility ) {
            logger.debug("### The table {} does not belong to the inner join view.", triggerRequest.getBaseTableKeySpace()
                    + "." + triggerRequest.getBaseTableName());
            return true;
        }

        Map<String, ColumnDefinition> reverseJoinTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                reverseJoinTable.getKeySpace(), reverseJoinTable.getName());
        for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : reverseJoinTableDesc.entrySet() ) {
            if ( columnDefinitionEntry.getValue().isPartitionKey() ) {
                innerJoinViewPrimaryKey.setColumnInternalCassType(columnDefinitionEntry.getValue().type.toString());
                innerJoinViewPrimaryKey.setColumnJavaType(ViewMaintenanceUtilities.getJavaTypeFromCassandraType(
                        innerJoinViewPrimaryKey.getColumnInternalCassType()));
                innerJoinViewPrimaryKey.setColumnName(columnDefinitionEntry.getKey());
            }

            while ( dataIter.hasNext() ) {
                List<String> tempColDescList = new ArrayList<>();
                String tempDataKey = (String) dataIter.next();
                logger.debug("Key: " + tempDataKey);
                logger.debug("Value: " + dataJson.get(tempDataKey));

//                if (tempDataKey.equalsIgnoreCase(columnDefinitionEntry.getKey())) {
//
//                } else if ((columnDefinitionEntry.getKey() + "_" + triggerRequest.getBaseTableName()).equalsIgnoreCase(
//                        tempDataKey)) {
//
//                }

                if ( innerJoinViewPrimaryKey.getColumnName().equalsIgnoreCase(tempDataKey) ) {
                    if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                        innerJoinViewPrimaryKey.setColumnValueInString((String) dataJson.get(tempDataKey));
                    } else if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                        innerJoinViewPrimaryKey.setColumnValueInString(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                    }
                }

            }
        }

        if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            reverseJoinRecordFetchQuery = QueryBuilder.select().all().from(reverseJoinTable.getKeySpace(),
                    reverseJoinTable.getName()).where(QueryBuilder.eq(innerJoinViewPrimaryKey.getColumnName(),
                    Integer.parseInt(innerJoinViewPrimaryKey.getColumnValueInString())));
        } else if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            reverseJoinRecordFetchQuery = QueryBuilder.select().all().from(reverseJoinTable.getKeySpace(),
                    reverseJoinTable.getName()).where(QueryBuilder.eq(innerJoinViewPrimaryKey.getColumnName(),
                    innerJoinViewPrimaryKey));
        }

        List<Row> existingReverseJoinRecords = CassandraClientUtilities.commandExecution("localhost",
                reverseJoinRecordFetchQuery);
        boolean insertToInnerJoinEligible = true;
        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();

        if ( existingReverseJoinRecords.size() > 0 ) {
            Row existingReverseJoinRecord = existingReverseJoinRecords.get(0);
            for ( Column column: operationViewTables.get(0).getColumns()) {
                columnNames.add(column.getName());
                if (column.isPrimaryKey()) {
                    if (column.getJavaDataType().equalsIgnoreCase("Integer")){
                        if (existingReverseJoinRecord.getInt(column.getName()) == 0) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            objects.add(existingReverseJoinRecord.getInt(column.getName()));
                        }
                    } else if (column.getJavaDataType().equalsIgnoreCase("String")) {
                        if (existingReverseJoinRecord.getString(column.getName()) == null) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            objects.add(existingReverseJoinRecord.getString(column.getName()));
                        }
                    }
                } else {
                    if (column.getDataType().equalsIgnoreCase("map<int, text>")) {
                        if (existingReverseJoinRecord.getMap(column.getName(), Integer.class, String.class) == null) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            objects.add(existingReverseJoinRecord.getMap(column.getName(), Integer.class, String.class));
                        }
                    } else if (column.getDataType().equalsIgnoreCase("map<int, int>")) {
                        if (existingReverseJoinRecord.getMap(column.getName(), Integer.class, Integer.class) == null) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            objects.add(existingReverseJoinRecord.getMap(column.getName(), Integer.class, Integer.class));
                        }
                    }
                }
            }
        }

        if (insertToInnerJoinEligible) {
            // Insert into the inner join view table

            Statement insertQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).values((String[])columnNames.toArray(),
                    objects.toArray());

            logger.debug("#### Insert query in innerjoin view table :: " + insertQuery);

            CassandraClientUtilities.commandExecution("localhost", insertQuery);


         } else {
            logger.debug("#### View maintenance not required as the row does not qualify as inner join ");
            return true;
        }


        return false;
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
        return "InnerJoinOperation{" +
                "\n deltaTableRecord=" + deltaTableRecord +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }
}
