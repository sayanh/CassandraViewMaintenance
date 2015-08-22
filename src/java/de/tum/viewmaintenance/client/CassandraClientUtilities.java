package de.tum.viewmaintenance.client;

/**
 * Created by shazra on 6/21/15.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class CassandraClientUtilities {
    protected static final Logger logger = LoggerFactory.getLogger(CassandraClientUtilities.class);

    /**
     * This method creates a keyspace if it is not present in a Cassandra instance
     **/
    public static boolean createKeySpace(Cluster cluster, String keyspace) {
        boolean isSucc = false;
        try {
            logger.debug("Creating keyspace {}", keyspace);
            String query = "CREATE SCHEMA IF NOT EXISTS " +
                    keyspace + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
            isSucc = CassandraClientUtilities.commandExecution(cluster, query);
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        }
        return isSucc;
    }

    /*
    * This method creates a connection to a Cassandra instance and returns the cluster
    */
    public static Cluster getConnection(String ip) {
        Session session = null;
        Cluster cluster = null;
        try {
            ResultSet results;
            Row rows;
            cluster = Cluster.builder()
                    .addContactPoint("localhost")
                    .build();
        } catch (Exception e) {
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            e.printStackTrace();
        }
        return cluster;
    }

    public static boolean closeConnection(Cluster cluster) {
        try {
            if (cluster.isClosed()) {
                cluster.close();
            }
            logger.info("Connection is successfully closed!!");
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        }
        return true;
    }

    /*
    * This method creates a table in a Cassandra instance
    */
    public static boolean createTable(Cluster cluster, Table table) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuffer query = new StringBuffer();
            query.append("create table if not exists " + table.getKeySpace() + "." + table.getName() + " (");
            List<Column> columns = table.getColumns();
            for (Column col : columns) {
                String primaryKey = col.isPrimaryKey() ? " PRIMARY KEY" : "";
                query.append(col.getName() + " " + col.getDataType() + primaryKey + ",");
            }
            String finalQuery = query.substring(0, query.length() - 1) + ");";
            System.out.println("Final query = " + finalQuery);
            results = session.execute(finalQuery);

            logger.debug("Successfully created table {}.{}", table.getKeySpace(), table.getName());

        } catch (Exception e) {
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        } finally {
            if (session.isClosed()) {
                session.close();
            }
        }

        return true;
    }

    /**
     * Deletes a table when the inputs are the keyspace and table name
     *
     * **/

    public static boolean deleteTable(Cluster cluster, String keyspace, String tableName){
        Table tempTable = new Table();
        tempTable.setKeySpace(keyspace);
        tempTable.setName(tableName);
        return deleteTable(cluster, tempTable);
    }

    /*
    * This method deletes a table from a Cassandra instance
    */
    public static boolean deleteTable(Cluster cluster, Table table) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuffer query = new StringBuffer();
            query.append("drop table " + table.getKeySpace() + "." + table.getName() + ";");

            System.out.println("Final query = " + query);
            results = session.execute(query.toString());

            logger.debug("Successfully delete table {}.{}", table.getKeySpace(), table.getName());

        } catch (Exception e) {
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        } finally {
            session.close();
        }

        return true;
    }

    /*
    * This method checks the presence of a table in a Cassandra instance
    *
    */
    public static boolean searchTable(Cluster cluster, Table table) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuffer query = new StringBuffer();
            query.append("Select columnfamily_name from system.schema_columnfamilies where columnfamily_name = '" + table.getName() + "' ALLOW FILTERING ;");

            System.out.println("Final query = " + query);
            results = session.execute(query.toString());
            String resultString = results.all().toString();
            logger.debug("Resultset {}", resultString);
            if (resultString.contains(table.getName())) {
                return true;
            }

        } catch (Exception e) {
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        } finally {
            if (session.isClosed()) {
                session.close();
            }
        }

        return false;
    }

    /*
    * This method executes any CQL3 query on a Cassandra instance
    *
    */
    public static boolean commandExecution(Cluster cluster, String query) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            logger.debug("Final query = " + query);
            results = session.execute(query);
            String resultString = results.all().toString();

        } catch (Exception e) {
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }

        return true;
    }

    public static boolean commandExecution(String ip, String query) {
        boolean isResultSuccessful = false;
        Cluster cluster = null;
        try {
            cluster = CassandraClientUtilities.getConnection(ip);
            isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query);
        } catch (Exception e) {
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            return false;
        } finally {
            if (!cluster.isClosed()) {
                CassandraClientUtilities.closeConnection(cluster);
            }
        }
        return true;
    }


    public static List<Row> commandExecution(String ip, Statement query) {
        Cluster cluster = null;
        List<Row> result = null;
        Session session = null;
        try {
            cluster = CassandraClientUtilities.getConnection(ip);
            session = cluster.connect();
            result = session.execute(query).all();
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + e.getMessage());
            return null;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            if (!cluster.isClosed()) {
                CassandraClientUtilities.closeConnection(cluster);
            }
        }
        return result;
    }

    public static List<Row> getAllRows(String keyspace, String table , Clause equal) {
        Cluster cluster = null;
        Session session = null;
        List<Row> result = null;
        try {
            cluster = CassandraClientUtilities.getConnection("localhost");
            session = cluster.connect();
            Statement statement = null;
            if (equal == null) {
                statement = QueryBuilder
                        .select()
                        .all()
                        .from(keyspace, table);
            } else {
                statement = QueryBuilder
                        .select()
                        .all()
                        .from(keyspace, table).
                                where(equal);
            }

            logger.debug("Final statement got executed : " + statement);
            result = session
                    .execute(statement)
                    .all();
            session.close();
            cluster.close();


        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
        } finally {
            if (session.isClosed()) {
                session.close();
            }

            if (cluster.isClosed()) {
                cluster.close();
            }
        }
        return  result;
    }


    static Table createDeltaViewTable(Table baseTable) {
        Table viewTable = new Table();
        viewTable.setName(baseTable.getName() + "_deltaView");
        viewTable.setKeySpace(baseTable.getKeySpace());
        List<Column> columns = baseTable.getColumns();
        List<Column> viewTableCols = new ArrayList<>();
        System.out.println("columns size = " + columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            System.out.println("working on col = " + col.getName());
            Column viewCol = new Column();
            if (col.isPrimaryKey()) {
                viewCol.setName(col.getName());
                viewCol.setIsPrimaryKey(col.isPrimaryKey());

            } else {
                Column viewCol_cur = new Column();
                viewCol_cur.setName(col.getName() + "_cur");
                viewCol.setName(col.getName() + "_last");
                viewCol_cur.setDataType(col.getDataType());
                viewTableCols.add(viewCol_cur);

            }

            viewCol.setDataType(col.getDataType());
            viewTableCols.add(viewCol);
        }
        viewTable.setColumns(viewTableCols);
        return viewTable;
    }


}
