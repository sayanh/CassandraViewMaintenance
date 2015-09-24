package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Statement;

import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 9/24/15.
 */
public class WhereUtilityProcessor {
    Map<String, List<String>> columnMap;
    Statement fetchExistingRecordQuery;
    PrimaryKey wherePrimaryKey;

    public Map<String, List<String>> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, List<String>> columnMap) {
        this.columnMap = columnMap;
    }


    public Statement getFetchExistingRecordQuery() {
        return fetchExistingRecordQuery;
    }

    public void setFetchExistingRecordQuery(Statement fetchExistingRecordQuery) {
        this.fetchExistingRecordQuery = fetchExistingRecordQuery;
    }

    public PrimaryKey getWherePrimaryKey() {
        return wherePrimaryKey;
    }

    public void setWherePrimaryKey(PrimaryKey wherePrimaryKey) {
        this.wherePrimaryKey = wherePrimaryKey;
    }
}
