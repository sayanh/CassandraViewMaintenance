package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ConstraintsTypes;
import de.tum.viewmaintenance.config.PrimaryKey;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by shazra on 6/27/15.
 */
public class SelectTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(SelectTrigger.class);

    /*
    *
    * This method is triggered when an insert query is made by a client.
    * It ensures that the select views are consistent.
    *
    */
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt1 -> This view performs "select"
        logger.debug("**********Insert Trigger for select view maintenance**********");
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table table = request.getViewTable();
        StringBuffer query = new StringBuffer("Insert into " + request.getViewKeyspace() + "." + table.getName() + " ( ");
        StringBuffer valuesPartQuery = new StringBuffer("values ( ");
        List<Column> columns = table.getColumns();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
//        String colAggKey = "";
        Statement insertQuery = null;
        boolean insertGreaterFlag = false;
        boolean insertLesserFlag = false;

        List<String> colNames = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        int age = 0;

        while ( dataIter.hasNext() ) {
            String tempDataKey = (String) dataIter.next();
            logger.debug("Key: " + tempDataKey);
            logger.debug("Value: " + dataJson.get(tempDataKey));

            if ( tempDataKey.equals("user_id") ) {
                tempUserId = (String) dataJson.get(tempDataKey);
            } else if ( tempDataKey.equals("age") ) {
                age = Integer.parseInt((String) dataJson.get(tempDataKey));
            } else if ( tempDataKey.equals("colaggkey_x") ) {
//                colAggKey = (String) dataJson.get(tempDataKey);
            }
        }

        PrimaryKey primaryKeySelect = ViewMaintenanceUtilities.getPrimaryKeyFromTableConfigWithoutValue(
                request.getViewKeyspace(), table.getName());

        primaryKeySelect.setColumnValueInString(tempUserId);
        Row existingRecord = null;

        try {
            existingRecord = ViewMaintenanceUtilities.getExistingRecordIfExists(primaryKeySelect,
                    table);

            logger.debug("#### Existing record : " + existingRecord);
        } catch ( SocketException e ) {
            logger.error("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
        }


        // Traversing the view table based on its columns
        for ( int i = 0; i < columns.size(); i++ ) {
            Column tempCol = columns.get(i);
            if ( tempCol.getName().equalsIgnoreCase("select_view1_age") ) {
                // No constraint case
                valuesPartQuery.append(age + ", ");
                query.append(tempCol.getName() + ", ");
            } else if ( tempCol.getName().equalsIgnoreCase("select_view2_age") ) {
                // greater than case
                String constraintArr[] = tempCol.getConstraint().split(" ");
                if ( constraintArr.length == 2 ) {
                    if ( constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.GREATER_THAN)) ) {

                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if ( age > constraintNum ) {
                            if ( !insertGreaterFlag ) {
                                insertGreaterFlag = true;
                            }
                            valuesPartQuery.append(age + ", ");
                            query.append(tempCol.getName() + ", ");
                            colNames.add("select_view2_age");
                            objects.add(age);

                        } else {
//                            query.append(tempCol.getName() + ", ");
//                            valuesPartQuery.append("null" + ", ");
                        }
                    }
                }


            } else if ( tempCol.getName().equalsIgnoreCase("select_view3_age") ) {
                // less than case

                String constraintArr[] = tempCol.getConstraint().split(" ");
                if ( constraintArr.length == 2 ) {
                    if ( constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.LESS_THAN)) ) {

                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if ( age < constraintNum ) {
                            if ( !insertLesserFlag ) {
                                insertLesserFlag = true;
                            }
                            valuesPartQuery.append(age + ", ");
                            query.append(tempCol.getName() + ", ");
                        } else {
                            query.append(tempCol.getName() + ", ");
                            valuesPartQuery.append("null" + ", ");
                        }
                    }
                }

            } else if ( tempCol.getName().equalsIgnoreCase("k") ) {
                // primary key case
                query.append(tempCol.getName() + ", ");
                valuesPartQuery.append(tempUserId + ", ");
                colNames.add("k");
                objects.add(Integer.parseInt(tempUserId));
            }
        }

        //query.append(" ) " + valuesPartQuery.toString() + " )");

        if ( insertGreaterFlag || insertLesserFlag ) {
            insertQuery = QueryBuilder.insertInto(request.getViewKeyspace(),
                    table.getName()).values(colNames.toArray(new String[colNames.size()]), objects.toArray());
            logger.debug("******* Insert query : " + insertQuery.toString());
            try {
//            isResultSuccessful = CassandraClientUtilities.commandExecution(
//                    CassandraClientUtilities.getEth0Ip(), query.toString());
                CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(),
                        insertQuery);
                isResultSuccessful = true;
            } catch ( SocketException e ) {
                logger.debug("Error !!" + ViewMaintenanceUtilities.getStackTrace(e));
            }
        } else {
            if ( existingRecord != null ) {
                Statement deleteQuery = QueryBuilder.delete().from(request.getViewKeyspace(), table.getName())
                        .where(QueryBuilder.eq("k", Integer.parseInt(tempUserId)));

                logger.debug("###  delete query :: " + deleteQuery);
                try {
                    CassandraClientUtilities.deleteCommandExecution(CassandraClientUtilities.getEth0Ip(),
                            deleteQuery);
                } catch ( SocketException e ) {
                    logger.debug("Error !!" + ViewMaintenanceUtilities.getStackTrace(e));
                }
            }
        }
        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(true);
        response.setIsSuccess(isResultSuccessful);
        return response;
    }

    /*
    *
    * This method is triggered when an update query is made by a client.
    * It ensures that the select views are consistent.
    *
    */

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        // vt1 -> This view performs select
        logger.debug("**********Update Trigger for select view maintenance**********");
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table table = request.getViewTable();
        StringBuffer query = new StringBuffer("update " + request.getViewKeyspace() + "." + table.getName() + " set ");
        List<Column> columns = table.getColumns();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        int age = 0;
        String whereString = request.getWhereString();
        while ( dataIter.hasNext() ) {
            String tempDataKey = (String) dataIter.next();
            logger.debug("Key: " + tempDataKey);
            logger.debug("Value: " + dataJson.get(tempDataKey));

            if ( tempDataKey.equals("user_id") ) {
            } else if ( tempDataKey.equals("age") ) {
                age = Integer.parseInt((String) dataJson.get(tempDataKey));
            }
        }

        // Traversing the view table based on its columns
        for ( int i = 0; i < columns.size(); i++ ) {
            Column tempCol = columns.get(i);
            if ( tempCol.getName().equalsIgnoreCase("select_view1_age") ) {
                // No constraint case
                query.append(tempCol.getName() + " = " + age + ", ");
            } else if ( tempCol.getName().equalsIgnoreCase("select_view2_age") ) {
                // greater than case
                String constraintArr[] = tempCol.getConstraint().split(" ");
                if ( constraintArr.length == 2 ) {
                    if ( constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.GREATER_THAN)) ) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if ( age > constraintNum ) {
                            query.append(tempCol.getName() + " = " + age + ", ");
                        } else {
                            query.append(tempCol.getName() + " = null, ");
                        }
                    }
                }


            } else if ( tempCol.getName().equalsIgnoreCase("select_view3_age") ) {
                // less than case

                String constraintArr[] = tempCol.getConstraint().split(" ");
                if ( constraintArr.length == 2 ) {
                    if ( constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.LESS_THAN)) ) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if ( age < constraintNum ) {
                            query.append(tempCol.getName() + " = " + age + ", ");
                        } else {
                            query.append(tempCol.getName() + " = null, ");
                        }
                    }
                }

            }
        }

        //TODO: Run time construction of the query by checking for the table structure and primary key
        if ( query.lastIndexOf(", ") == query.length() - 2 ) {
            query.delete(query.length() - 2, query.length());
        }

        // TODO: Need to configure primary keys for both base table and view table. Right now it is hardcoded.
        query.append(" " + whereString.replace("user_id", "k"));
        logger.debug("******* Query : " + query.toString());
        try {
            isResultSuccessful = CassandraClientUtilities.commandExecution(
                    CassandraClientUtilities.getEth0Ip(), query.toString());
        } catch ( SocketException e ) {
            logger.debug("Error !!" + ViewMaintenanceUtilities.getStackTrace(e));
        }
        TriggerResponse response = new TriggerResponse();
//        response.setIsSuccess(true);
        response.setIsSuccess(isResultSuccessful);
        return response;
    }



    /*
    *
    * This method is triggered when an delete query is made by a client.
    * It ensures that the select views are consistent.
    *
    */

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        logger.debug("**********Delete Trigger for Select view maintenance**********");
        TriggerResponse response = new TriggerResponse();
        boolean isResultSucc = false;
        String whereString = request.getWhereString().replace("user_id", "k");
        Table table = request.getViewTable();
        try {
            String deleteQuery = "delete from " + request.getViewKeyspace() + "." + table.getName() + " " + whereString;
            logger.debug("Delete query for select view maintenance : " + deleteQuery);
            isResultSucc = CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), deleteQuery);
            response.setIsSuccess(isResultSucc);
        } catch ( Exception e ) {
            logger.error("Error !!!" + ViewMaintenanceUtilities.getStackTrace(e));
            response.setIsSuccess(false);
        }
        return response;
    }


}
