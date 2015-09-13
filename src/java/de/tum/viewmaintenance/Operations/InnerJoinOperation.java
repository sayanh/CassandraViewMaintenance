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

    public static InnerJoinOperation getInstance(List<Table> inputViewTable,
                                                 List<Table> operationViewTable) {
        InnerJoinOperation innerJoinOperation = new InnerJoinOperation();
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
        this.deltaTableRecord = triggerRequest.getCurrentRecordInDeltaView();
        logger.debug("##### Delta table record {}", this.deltaTableRecord);
        Table reverseJoinTable = inputViewTables.get(0);
        LinkedTreeMap dataJson = triggerRequest.getDataJson();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        PrimaryKey innerJoinViewPrimaryKey = null;
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
        } else {
            logger.debug("### Inner join view maintenance starts for {}", triggerRequest.getBaseTableKeySpace() + "." +
                    triggerRequest.getBaseTableName());
        }

        Map<String, ColumnDefinition> reverseJoinTableDesc = ViewMaintenanceUtilities.getTableDefinitition(
                reverseJoinTable.getKeySpace(), reverseJoinTable.getName());
        for ( Map.Entry<String, ColumnDefinition> columnDefinitionEntry : reverseJoinTableDesc.entrySet() ) {
            if ( columnDefinitionEntry.getValue().isPartitionKey() ) {
                innerJoinViewPrimaryKey = new PrimaryKey(columnDefinitionEntry.getKey(), columnDefinitionEntry.getValue()
                        .type.toString(), "");

                while ( dataIter.hasNext() ) {
                    String tempDataKey = (String) dataIter.next();
                    logger.debug("Key: " + tempDataKey);
                    logger.debug("Value: " + dataJson.get(tempDataKey));

                    if ( innerJoinViewPrimaryKey.getColumnName().equalsIgnoreCase(tempDataKey) ) {
                        if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
                            innerJoinViewPrimaryKey.setColumnValueInString((String) dataJson.get(tempDataKey));
                        } else if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
                            innerJoinViewPrimaryKey.setColumnValueInString(((String) dataJson.get(tempDataKey)).replaceAll("'", ""));
                        }
                        break;
                    }
                }
            }

        }

        logger.debug("### Inner join primary key = " + innerJoinViewPrimaryKey);

        if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("Integer") ) {
            reverseJoinRecordFetchQuery = QueryBuilder.select().all().from(reverseJoinTable.getKeySpace(),
                    reverseJoinTable.getName()).where(QueryBuilder.eq(innerJoinViewPrimaryKey.getColumnName(),
                    Integer.parseInt(innerJoinViewPrimaryKey.getColumnValueInString())));
        } else if ( innerJoinViewPrimaryKey.getColumnJavaType().equalsIgnoreCase("String") ) {
            reverseJoinRecordFetchQuery = QueryBuilder.select().all().from(reverseJoinTable.getKeySpace(),
                    reverseJoinTable.getName()).where(QueryBuilder.eq(innerJoinViewPrimaryKey.getColumnName(),
                    innerJoinViewPrimaryKey));
        }


        logger.debug("#### reverseJoinRecordFetchQuery  || " + reverseJoinRecordFetchQuery);

        List<Row> existingReverseJoinRecords = CassandraClientUtilities.commandExecution("localhost",
                reverseJoinRecordFetchQuery);
        boolean insertToInnerJoinEligible = true;
        List<String> columnNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        //TODO: Remove all the null checks and use is NUll
        //TODO: If a column is null then break else check for type and get the values which will be inserted to innerjoin view table
        if ( existingReverseJoinRecords != null && existingReverseJoinRecords.size() > 0 ) {
            Row existingReverseJoinRecord = existingReverseJoinRecords.get(0);
            for ( Column column : operationViewTables.get(0).getColumns() ) {
                logger.debug("#### Checking | executing column :: " + column.getDataType());
                if ( column.isPrimaryKey() ) {
                    String javaDataTypePK = ViewMaintenanceUtilities.getJavaDataTypeFromCQL3DataType(column.getDataType());
                    if ( javaDataTypePK.equalsIgnoreCase("Integer") ) {
                        if ( existingReverseJoinRecord.getInt(column.getName()) == 0 ) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getInt(column.getName()));
                        }
                    } else if ( javaDataTypePK.equalsIgnoreCase("String") ) {
                        if ( existingReverseJoinRecord.isNull(column.getName()) ) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getString(column.getName()));
                        }
                    }
                } else if ( column.getDataType().equalsIgnoreCase("list<int>") ) {
                    // For the actual primary key of datatype int
                    List<Integer> actualPKListInReverseJoinView = existingReverseJoinRecord.getList(column.getName(),
                            Integer.class);
                    logger.debug("#### Checking : actualPrimaryKeyCol(list) in ReverseJoin Table : "
                            + actualPKListInReverseJoinView);
                    if ( actualPKListInReverseJoinView == null || actualPKListInReverseJoinView.isEmpty() ) {
                        insertToInnerJoinEligible = false;
                        break;
                    } else {
                        columnNames.add(column.getName());
                        objects.add(existingReverseJoinRecord.getList(column.getName(), Integer.class));
                    }

                } else if ( column.getDataType().equalsIgnoreCase("list<text>") ) {
                    // For the actual primary key of datatype String
                    List<String> actualPKListInReverseJoinView = existingReverseJoinRecord.getList(column.getName(), String.class);
                    logger.debug("#### Checking : actualPrimaryKeyCol(list) in ReverseJoin Table : " + actualPKListInReverseJoinView);
                    if ( actualPKListInReverseJoinView == null || actualPKListInReverseJoinView.isEmpty() ) {
                        insertToInnerJoinEligible = false;
                        break;
                    } else {
                        columnNames.add(column.getName());
                        objects.add(existingReverseJoinRecord.getList(column.getName(), String.class));
                    }
                } else {
                    if ( column.getDataType().equalsIgnoreCase("map<int,text>") ) {
                        Map<Integer, String> reverseJoinMap = existingReverseJoinRecord.getMap(column.getName(),
                                Integer.class, String.class);
                        if ( reverseJoinMap == null || reverseJoinMap.isEmpty() ) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getMap(column.getName(), Integer.class, String.class));
                        }
                    } else if ( column.getDataType().equalsIgnoreCase("map<int,int>") ) {
                        Map<Integer, Integer> reverseJoinMap = existingReverseJoinRecord.getMap(column.getName(),
                                Integer.class, Integer.class);
                        logger.debug("#### Checking : reverseJoinMap : " + reverseJoinMap);
                        if ( reverseJoinMap == null || reverseJoinMap.isEmpty() ) {
                            insertToInnerJoinEligible = false;
                            break;
                        } else {
                            columnNames.add(column.getName());
                            objects.add(existingReverseJoinRecord.getMap(column.getName(), Integer.class, Integer.class));
                        }
                    }
                }
            }
        }

        logger.debug("#### insertToInnerJoinEligible :: " + insertToInnerJoinEligible);

        if ( insertToInnerJoinEligible ) {
            // Insert into the inner join view table

            Statement insertQuery = QueryBuilder.insertInto(operationViewTables.get(0).getKeySpace(),
                    operationViewTables.get(0).getName()).values(columnNames.toArray(new String[columnNames.size()]),
                    objects.toArray());

            logger.debug("#### Insert query in innerjoin view table :: " + insertQuery);

            CassandraClientUtilities.commandExecution("localhost", insertQuery);

        } else {
            logger.debug("#### View maintenance not required as the row does not qualify as inner join ");
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
        return "InnerJoinOperation{" +
                ",\n inputViewTables=" + inputViewTables +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }
}
