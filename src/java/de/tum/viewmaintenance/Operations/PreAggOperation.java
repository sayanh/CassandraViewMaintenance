package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;

/**
 * Created by shazra on 8/14/15.
 */
public class PreAggOperation extends GenericOperation {

    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private Table inputViewTable;
    private Table operationViewTable;

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public GenericOperation getSqlOperation() {
        return sqlOperation;
    }

    public void setSqlOperation(GenericOperation sqlOperation) {
        this.sqlOperation = sqlOperation;
    }

    public Table getInputViewTable() {
        return inputViewTable;
    }

    public void setInputViewTable(Table inputViewTable) {
        this.inputViewTable = inputViewTable;
    }

    public Table getOperationViewTable() {
        return operationViewTable;
    }

    public void setOperationViewTable(Table operationViewTable) {
        this.operationViewTable = operationViewTable;
    }

    private PreAggOperation(){
        super();
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    @Override
    public GenericOperation getInstance(Row deltaTableRecord, Table inputViewTable,
                                        Table operationViewTable, GenericOperation sqlOperation) {
        PreAggOperation PreAggOperation = new PreAggOperation();
        PreAggOperation.setDeltaTableRecord(deltaTableRecord);
        PreAggOperation.setInputViewTable(inputViewTable);
        PreAggOperation.setOperationViewTable(operationViewTable);
        PreAggOperation.setSqlOperation(sqlOperation);
        return PreAggOperation;
    }

    @Override
    public boolean insertTrigger() {
        return false;
    }

    @Override
    public boolean updateTrigger() {
        return false;
    }

    @Override
    public boolean deleteTrigger() {
        return false;
    }

}
