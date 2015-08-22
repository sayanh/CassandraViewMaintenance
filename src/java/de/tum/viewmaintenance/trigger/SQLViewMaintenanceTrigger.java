package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.Operations.*;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by anarchy on 8/2/15.
 */
public class SQLViewMaintenanceTrigger extends TriggerProcess{
    private static final Logger logger = LoggerFactory.getLogger(SQLViewMaintenanceTrigger.class);
    private Map<String, Boolean> operationsFileMap = null;
    private List<GenericOperation> operationQueue = new ArrayList<>();
    private static final String OPERATIONS_FILENAME = "logicalplan.json";

    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        return null;
    }

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        return null;
    }

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        return null;
    }


    /**
     * Here viewConfig means the view config read from the config file.
     * This is not a view table as the other standalone views.
     * This in turn produces a series of views.
     **/
    public TriggerResponse processSQLViewMaintenance(String type, Table viewConfig,
                                                     Row deltaTableViewRow, TriggerRequest triggerRequest)
            throws IOException, JSQLParserException {
        logger.debug("ProcessSQLViewMaintenace | with type: {} , viewConfig {} , deltaTableViewRow {} ", type, viewConfig, deltaTableViewRow);
        /**
         *  Decides and creates the view table names, structure.
         * */

        if (operationQueue.size() == 0) {
            createSQLTables(viewConfig, deltaTableViewRow);
        }


        /**
         * View Maintenance Process starts
         * **/
        processTriggersForViewMaintenance(type, triggerRequest);

        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(true);
        return response;
    }

    private void processTriggersForViewMaintenance(String type, TriggerRequest triggerRequest) {
        logger.debug("#### processTriggersForViewMaintenance ### " +
                "Available Operations are #####");

        for (GenericOperation operation: operationQueue){
            logger.debug("### Checking - Operation class = {}", operation);

            operation.processOperation(type, triggerRequest);
        }
    }


    /**
     * TODO: This will not work for nested SELECT statements. For that following action should be taken.
     * <view_name>_<operation_name>_<basetable_name>_<counter>
     * Each time due to a nested select query, it reaches here, the counter increases.
     *
     * **/
    private List<Table> createSQLTables(Table viewConfig, Row deltaTableViewRow) throws IOException, JSQLParserException {
        logger.debug(" ***** Inside createSQLTables() ..... for view: ", viewConfig.getName());
        ResultViewTable resultViewTable = null;

//        try {
        String sqlString = viewConfig.getSqlString();

        // operationsInvolved facilitates random and quick check on the presence of the clauses present.
        Map<String, String> operationsInvolved = new HashMap<>();


        List<Function> functionList = new ArrayList<>();
        List<Expression> listSelectExpressions = new ArrayList<>();

        String baseFromTableName = "";
        String baseFromKeySpace = "";
        Statement stmt = CCJSqlParserUtil.parse(sqlString);
        PlainSelect plainSelect = null;

        WhereViewTable whereViewTable = null;
        ReverseJoinViewTable reverseJoinViewTable = null;
        InnerJoinViewTable innerJoinViewTable = null;
        InnerJoinOperation innerJoinOperation = null;
        PreAggViewTable preAggViewTable = null;
        AggViewTable aggViewTable = null;

        logger.debug("### The current status of the operationQueue is ### " + operationQueue);
        logger.debug("### The current size of the operationQueue is ### " + operationQueue.size());


        if (operationQueue.size() == 0) {

            logger.debug(" ****** Operation Queue is null:: Entering here for first time ******");
            if (stmt instanceof Select) {
                Select select = (Select) stmt;
                plainSelect = (PlainSelect) select.getSelectBody();
            }

            logger.debug("### ### ### ### ###");
            logger.debug("### State of art ###");
            logger.debug("### Where clause: " + plainSelect.getWhere());
            logger.debug("### From clause: " + plainSelect.getFromItem());
            logger.debug("### Select items clause: " + plainSelect.getSelectItems());
            logger.debug("### Join clause: " + plainSelect.getJoins());
            logger.debug("### GroupBy clause: " + plainSelect.getGroupByColumnReferences());
            logger.debug("### Having clause: " + plainSelect.getHaving());
            logger.debug("### ### ### ### ###");

            /**
             * Checking for where clause
             **/


            if (plainSelect.getWhere() != null) {
                logger.debug("### Computing the where clause ###");
                String whereColName = "";

                Expression expression = plainSelect.getWhere();

                operationsInvolved.put("where", whereColName);


                for (SelectItem selectItem: plainSelect.getSelectItems()) {
                    if (selectItem instanceof SelectExpressionItem) {
                        SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
                        listSelectExpressions.add(expressionItem.getExpression());
                        if (expressionItem.getExpression() instanceof Function) {
                            Function function = (Function)expressionItem.getExpression();
                            functionList.add(function);
                        }
                    }
                }

                whereViewTable = new WhereViewTable();
                whereViewTable.setWhereExpressions(expression);
                whereViewTable.setShouldBeMaterialized(getMapOperations().get("where"));
                whereViewTable.setViewConfig(viewConfig);
                List<Table> whereTablesCreated = whereViewTable.createTable();
                if (whereViewTable.shouldBeMaterialized()) {
                    whereViewTable.materialize();
                } else {
                    //TODO: yet to be implemented.
                    whereViewTable.createInMemory(whereTablesCreated);
                }
                WhereOperation whereOperation = WhereOperation.getInstance(deltaTableViewRow, null, whereTablesCreated);
                whereOperation.setWhereExpression(expression);
                whereOperation.setViewConfig(viewConfig);
                operationQueue.add(whereOperation);
                logger.debug("### After adding where operation in operationQueue :: " + operationQueue);
            }

            if (plainSelect.getFromItem() instanceof net.sf.jsqlparser.schema.Table) {
                baseFromTableName = ((net.sf.jsqlparser.schema.Table) plainSelect.getFromItem()).getFullyQualifiedName();
                String baseFromTableNameArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(baseFromTableName);
                baseFromKeySpace = baseFromTableNameArr[0];
                baseFromTableName = baseFromTableNameArr[1];
                logger.debug("### From base table is ###  keyspace:{}, tableName:{} ", baseFromKeySpace, baseFromTableName);
                operationsInvolved.put("from", baseFromTableName);
            }


            /**
             * Checking for joins
             **/



            if (plainSelect.getJoins() != null) {
                /**
                 * Note: Assuming only one join will be present
                 **/

                // Creating ReverseJoin View

                reverseJoinViewTable = new ReverseJoinViewTable();
                reverseJoinViewTable.setJoins(plainSelect.getJoins());
                reverseJoinViewTable.setViewConfig(viewConfig);
                reverseJoinViewTable.setShouldBeMaterialized(getMapOperations().get("reversejoin"));
                reverseJoinViewTable.setFromBaseTable(baseFromKeySpace + "." + baseFromTableName);
                List<Table> reverseJoinTablesCreated = reverseJoinViewTable.createTable();
                logger.debug("### Checking - reversejoin shouldBeMaterialized() - " +
                        getMapOperations().get("reversejoin"));
                if (reverseJoinViewTable.shouldBeMaterialized()) {
                    reverseJoinViewTable.materialize();
                } else {
                    //TODO: yet to be implemented.
                    reverseJoinViewTable.createInMemory(reverseJoinTablesCreated);
                }

                ReverseJoinOperation reverseJoinOperation = ReverseJoinOperation.getInstance(deltaTableViewRow,
                        whereViewTable.getTables(),reverseJoinTablesCreated);
                operationQueue.add(reverseJoinOperation);
                operationsInvolved.put("join", getJoinType(plainSelect.getJoins().get(0)));

                // Creating Required Join View
                // Note: Only Inner Join works now

                List<Table> innerJoinTablesCreated = null;
                for (Join join: reverseJoinViewTable.getJoins()) {
                    if (join.isInner()) {
                        innerJoinViewTable = new InnerJoinViewTable();
                        innerJoinViewTable.setShouldBeMaterialized(getMapOperations().get("innerjoin"));
                        innerJoinViewTable.setInputReverseJoinTableStruc(
                                reverseJoinViewTable.getTables().get(0));
                        innerJoinViewTable.setViewConfig(viewConfig);
                        innerJoinTablesCreated = innerJoinViewTable.createTable();
                        logger.debug("### Checking - innerjoin shouldBeMaterialized() - " +
                                getMapOperations().get("innerjoin"));
                        if (innerJoinViewTable.shouldBeMaterialized()) {
                            innerJoinViewTable.materialize();
                        } else {
                            //TODO: yet to be implemented.
                            innerJoinViewTable.createInMemory(innerJoinTablesCreated);
                        }
                    }
                }

                innerJoinOperation = InnerJoinOperation.getInstance(deltaTableViewRow,
                        reverseJoinViewTable.getTables(), innerJoinTablesCreated);
                operationQueue.add(innerJoinOperation);

            }


            /**
             * Checking for aggregate functions
             * Clauses to check: aggregate functions in projection items, group by.
             * Assumption: If there is an aggregate function there has to be a group by associated with it.
             * Note: Only ONE groupBy reference works now.
             *
             * **/



            if (plainSelect.getGroupByColumnReferences() != null) {
                List<Expression> groupByExpressions = plainSelect.getGroupByColumnReferences();
                preAggViewTable = new PreAggViewTable();
                preAggViewTable.setDeltaTableRecord(deltaTableViewRow);
                preAggViewTable.setShouldBeMaterialized(getMapOperations().get("preaggregation"));
                if (operationsInvolved.get("join") != null) {
                    logger.debug(" ***** Join is present hence adding join view table :: " + innerJoinViewTable);
                    preAggViewTable.setInputViewTable(innerJoinViewTable);
                } else {
                    logger.debug(" ***** Join is not present hence adding where view table :: " + whereViewTable);
                    preAggViewTable.setInputViewTable(whereViewTable);
                }
                preAggViewTable.setViewConfig(viewConfig);
                preAggViewTable.setGroupByExpressions(groupByExpressions);
                preAggViewTable.setFunctionExpressions(functionList);
                preAggViewTable.setBaseTableName(baseFromKeySpace + "." + baseFromTableName);

                List<Table> preAggTablesCreated = preAggViewTable.createTable();
                logger.debug("### Checking - preaggregation shouldBeMaterialized() - " +
                        getMapOperations().get("preaggregation"));
                if (preAggViewTable.shouldBeMaterialized()) {
                    preAggViewTable.materialize();
                } else {
                    //TODO: yet to be implemented.
                    preAggViewTable.createInMemory(preAggTablesCreated);
                }

                if (operationsInvolved.get("join") != null) {
                    PreAggOperation preAggOperation = PreAggOperation.getInstance(deltaTableViewRow,
                            innerJoinViewTable.getTables(), preAggTablesCreated);
                } else {
                    PreAggOperation preAggOperation = PreAggOperation.getInstance(deltaTableViewRow,
                            whereViewTable.getTables(), preAggTablesCreated);
                }



                // Storing the expression in the operationsInvolved list.

                operationsInvolved.put("groupBy", groupByExpressions.get(0).toString());
            }

            /**
             * For cases when there is NO groupBy but there is an aggregate function
             * in the select item
             **/

//                if (!operationsInvolved.containsKey("groupBy") && ) {
//
//                }


            /**
             * Computing the aggregate view table
             **/

            if (plainSelect.getHaving() != null) {
                operationsInvolved.put("having", plainSelect.getHaving().toString());
                Expression expressionHaving = plainSelect.getHaving();

                aggViewTable = new AggViewTable();
                aggViewTable.setViewConfig(viewConfig);
                aggViewTable.setInputPreAggTableStruc(preAggViewTable.getTables().get(0));
                aggViewTable.setShouldBeMaterialized(getMapOperations().get("aggregation"));

                List<Table> aggViewTableCreated = aggViewTable.createTable();
                logger.debug("### Checking - aggregation shouldBeMaterialized() - " +
                        getMapOperations().get("aggregation"));
                if (aggViewTable.shouldBeMaterialized()) {
                    aggViewTable.materialize();
                } else {
                    //TODO: yet to be implemented.
                    aggViewTable.createInMemory(aggViewTableCreated);
                }

                AggOperation aggOperation = AggOperation.getInstance(deltaTableViewRow,
                        preAggViewTable.getTables(), aggViewTableCreated);
                operationsInvolved.put("having", expressionHaving.toString());
                operationQueue.add(aggOperation);
            }

            // Creation of views based on the functions present

            resultViewTable = new ResultViewTable();
            resultViewTable.setViewConfig(viewConfig);
            resultViewTable.setPlainSelect(plainSelect);
            resultViewTable.setBaseFromTableCompleteName(baseFromKeySpace + "." + baseFromTableName);

            List<Table> resultTableCreated = resultViewTable.createTable();
            logger.debug("### Materializing Result View Table :: " + resultTableCreated);
                resultViewTable.materialize();

            ResultViewOperation resultViewOperation = null;
            if (operationsInvolved.containsKey("having")) {
                resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                        aggViewTable.getTables(), resultTableCreated);
            } else if (operationsInvolved.containsKey("groupBy")){
                logger.debug("### Result operation depends on where and join");
                resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                        preAggViewTable.getTables(), resultTableCreated);
            } else if (operationsInvolved.containsKey("join")){
                logger.debug("### Result operation depends on where and join");
                resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                        innerJoinViewTable.getTables(), resultTableCreated);
            } else if (operationsInvolved.containsKey("where")){
                logger.debug("### Result operation depends on where");
                resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                        whereViewTable.getTables(), resultTableCreated);
            }

            operationQueue.add(resultViewOperation);
        }

//        } catch (JSQLParserException e) {
//            logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
//        }

        return resultViewTable.getTables();

    }


    private Map<String, Boolean> getMapOperations() throws IOException {
        if (operationsFileMap == null) {
            logger.debug("***** System.getProperty(\"user.dir\") = " + System.getProperty("user.dir") );
            String stringList = new String(Files.readAllBytes(Paths.get(OPERATIONS_FILENAME)));
            operationsFileMap = new Gson().fromJson(stringList, new TypeToken<HashMap<String, Object>>() {
            }.getType());
        }

        return operationsFileMap;
    }


    private String getJoinType(Join join) {
        if (join.isCross()) {
            return "CrossJoin";
        } else if (join.isFull()) {
            return "FullJoin";
        } else if (join.isInner()) {
            return "InnerJoin";
        } else if (join.isLeft()) {
            return "LeftJoin";
        } else if (join.isRight()) {
            return "RightJoin";
        }

        return "";

    }

}
