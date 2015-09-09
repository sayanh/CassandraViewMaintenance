package de.tum.viewmaintenance;

/**
 * Created by shazra on 9/6/15.
 */
public class FormatLog {
    private long timeTakenBatch;
    private long timeTakenView;
    private String viewName;
    private String sql;
    private int numberOfRowsProcessed;
    private String result;

    public void setResult(String result) {
        this.result = result;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setTimeTakenBatch(long timeTakenBatch) {
        this.timeTakenBatch = timeTakenBatch;
    }

    public void setTimeTakenView(long timeTakenView) {
        this.timeTakenView = timeTakenView;
    }

    public void setNumberOfRowsProcessed(int numberOfRowsProcessed) {
        this.numberOfRowsProcessed = numberOfRowsProcessed;
    }

    public String getPrettyLog() {
        StringBuffer sb = new StringBuffer();
        String delimiter = "|";

        sb.append("Output" + delimiter + viewName + delimiter + timeTakenBatch + delimiter + timeTakenView
                + delimiter + sql + delimiter + numberOfRowsProcessed + delimiter + result);

        return sb.toString();
    }
}
