package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;

import java.util.List;

/**
 * Created by shazra on 8/15/15.
 */

public class ReverseJoinOperation extends GenericOperation {
    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private List<Table> inputViewTable;
    private List<Table> operationViewTables;

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public GenericOperation getSqlOperation() {
        return sqlOperation;
    }

    public void setSqlOperation(GenericOperation sqlOperation) {
        this.sqlOperation = sqlOperation;
    }

    public List<Table> getInputViewTable() {
        return inputViewTable;
    }

    public void setInputViewTable(List<Table> inputViewTable) {
        this.inputViewTable = inputViewTable;
    }

    public List<Table> getOperationViewTables() {
        return operationViewTables;
    }

    public void setOperationViewTables(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    @Override
    public boolean insertTrigger(TriggerRequest triggerRequest) {
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

    public static ReverseJoinOperation getInstance(Row deltaTableRecord, List<Table> inputViewTable,
                                                   List<Table> operationViewTable){
        ReverseJoinOperation reverseJoinOperation = new ReverseJoinOperation();
        reverseJoinOperation.setDeltaTableRecord(deltaTableRecord);
        reverseJoinOperation.setInputViewTable(inputViewTable);
        reverseJoinOperation.setOperationViewTables(operationViewTable);
        return reverseJoinOperation;
    }

    @Override
    public String toString() {
        return "ReverseJoinOperation{" +
                "\n deltaTableRecord=" + deltaTableRecord +
                ",\n inputViewTable=" + inputViewTable +
                ",\n operationViewTables=" + operationViewTables +
                '}';
    }
}
