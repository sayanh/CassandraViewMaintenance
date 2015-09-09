package de.tum.viewmaintenance;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.cassandra.test.microbench.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 9/6/15.
 */
public class View13Test implements GenericViewTest {
    private static final Logger logger = LoggerFactory.getLogger(View13Test.class);
    Map<String, Integer> aggrSum = new HashMap<>();

    @Override
    public void startTest(Table viewConfig) {
        // sql = select emp.colaggkey_x, sum(emp.age) from schematest.emp where emp.age &gt; 10 group by emp.colaggkey_x having sum(emp.age) &gt; 20</SQL>
        long startTime = System.currentTimeMillis();
        int numOfRowsProcessed = processViewOperation();

        long stopTime = System.currentTimeMillis();
        long timetakenBatch = stopTime - startTime;
        logger.debug("### Time taken to fetch from batch and compute | " + timetakenBatch);
        FormatLog prettyLog = new FormatLog();
        prettyLog.setNumberOfRowsProcessed(numOfRowsProcessed);
        prettyLog.setSql(viewConfig.getSqlString());
        prettyLog.setTimeTakenBatch(timetakenBatch);
        prettyLog.setViewName(viewConfig.getName());
        Result result = matchExpectation();
        prettyLog.setResult(result.matched ? "true" : "false");
        prettyLog.setTimeTakenView(result.timeTakenView);
        logger.debug(prettyLog.getPrettyLog());
    }

    public int processViewOperation() {
        logger.debug(" #### Inside process View Operation #### ");
        int numberOfRowsProcessed = 0;
        Statement getAllRecordsQuery = QueryBuilder.select().all().from("schematest", "emp");
        logger.debug("getAllRecordsQuery :: " + getAllRecordsQuery);
        List<Row> allRecords = CassandraClientUtilities.commandExecution("localhost", getAllRecordsQuery);
        numberOfRowsProcessed = allRecords.size();

        // Group by
        for ( Row element : allRecords ) {
            int age = element.getInt("age");
            int newValue = 0;
            if ( age > 10 ) {
                if ( aggrSum.get(element.getString("colaggkey_x")) != null ) {
                    newValue = aggrSum.get(element.getString("colaggkey_x")) + age;
                }
                if ( newValue > 10 ) {
                    aggrSum.put(element.getString("colaggkey_x"), newValue);
                }
            }
        }
        return numberOfRowsProcessed;
    }

    public Result matchExpectation() {
        Result result = new Result();
        boolean match = true;

        try {
            logger.debug("###  Waiting to let the view maintenance to finish .....");
            Thread.sleep(20000);
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }

        long startTime = System.currentTimeMillis();
        Statement getRowsvt13ResultQuery = QueryBuilder.select().all().from("schema2", "vt13_result");
        logger.debug("getRowsvt13Result | " + getRowsvt13ResultQuery);
        List<Row> rowsvt13ResultRows = CassandraClientUtilities.commandExecution("localhost", getRowsvt13ResultQuery);
        long stopTime = System.currentTimeMillis();
        long timeInterval = stopTime - startTime;

        logger.debug("### Time taken to fetch from view | " + timeInterval);

        if ( aggrSum.size() == rowsvt13ResultRows.size() ) {
            for ( Row row : rowsvt13ResultRows ) {
                int sum = row.getInt("sum_age");
                String colAggKey = row.getString("colaggkey_x");
                if ( aggrSum.containsKey(colAggKey) && sum == aggrSum.get(colAggKey) ) {
                    logger.debug("#### Matched for agg col key : {} with value {} ### ", colAggKey, aggrSum.get(colAggKey));
                } else {
                    match = false;
                    break;
                }
            }
        } else {
            match = false;
        }


        logger.debug(" ##### Match expectation {} for View : vt13 #####", match);
        result.setMatched(match);

        result.setTimeTakenView(timeInterval);
        logger.debug("matchExpression | " + match);
        return result;
    }
}


class Result {
    boolean matched = false;
    long timeTakenView;

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

    public void setTimeTakenView(long timeTakenView) {
        this.timeTakenView = timeTakenView;
    }
}