package de.tum.viewmaintenance.view_table_structure;

import java.util.List;

/**
 * Created by anarchy on 8/24/15.
 */
public class LeftJoinViewTable implements ViewTable {
    @Override
    public List<Table> createTable() {
        return null;
    }

    @Override
    public void deleteTable() {

    }

    @Override
    public void materialize() {

    }

    @Override
    public boolean shouldBeMaterialized() {
        return false;
    }

    @Override
    public void createInMemory(List<Table> tables) {

    }
}
