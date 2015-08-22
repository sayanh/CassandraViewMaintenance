package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;

import java.util.List;

/**
 * Created by anarchy on 8/13/15.
 */
public abstract class GenericOperation {
    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private List<Table> inputViewTable;
    private List<Table> operationViewTable;

    public void processOperation(String type, TriggerRequest triggerRequest) {
        if (type.equalsIgnoreCase("insert")) {
            insertTrigger(triggerRequest);
        } else if (type.equalsIgnoreCase("update")) {
            updateTrigger(triggerRequest);
        } else if (type.equalsIgnoreCase("delete")) {
            deleteTrigger(triggerRequest);
        }
    }
    public abstract boolean insertTrigger(TriggerRequest triggerRequest);
    public abstract boolean updateTrigger(TriggerRequest triggerRequest);
    public abstract boolean deleteTrigger(TriggerRequest triggerRequest);

}
