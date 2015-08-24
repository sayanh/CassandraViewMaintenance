package de.tum.viewmaintenance.Operations;

import de.tum.viewmaintenance.trigger.TriggerRequest;

/**
 * Created by shazra on 8/24/15.
 */
public class LeftJoinOperation extends GenericOperation {
    @Override
    public void processOperation(String type, TriggerRequest triggerRequest) {
        super.processOperation(type, triggerRequest);
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
}
