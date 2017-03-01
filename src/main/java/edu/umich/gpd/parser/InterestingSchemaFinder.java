package edu.umich.gpd.parser;

import com.google.common.collect.*;
import edu.umich.gpd.main.GPDMain;
import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.workload.Query;
import edu.umich.gpd.workload.Workload;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import java.util.*;

/**
 * Find tables with their columns, which are in their 'interesting order'.
 * Created by Dong Young Yoon on 2/13/17.
 */
public class InterestingSchemaFinder implements StatementVisitor, SelectVisitor, OrderByVisitor,
    FromItemVisitor, ExpressionVisitor, ItemsListVisitor, IntoTableVisitor {

  private Set<String> currentTableSet;
  private Set<Set<String>> interestingTableSets;
  private Set<String> tables;
  private Multiset<String> columns;
  private boolean isInteresting;
  private boolean isJoin;
  private Query currentQuery;

  public InterestingSchemaFinder() {
    this.tables = new LinkedHashSet<>();
    this.currentTableSet = new LinkedHashSet<>();
    this.interestingTableSets = new LinkedHashSet<>();
    this.columns = ConcurrentHashMultiset.create();
    this.isInteresting = false;
    this.isJoin = false;
  }

  public Schema getFilteredSchema(Schema s) {
    if (s == null) {
      return null;
    }

    try {
      Schema filteredSchema = (Schema)s.clone();
      ImmutableMultiset<String> sortedColumns = Multisets.copyHighestCountFirst(columns);
      Set<String> sortedColumnSet = new LinkedHashSet<>();
      for (String col : sortedColumns) {
        sortedColumnSet.add(col);
      }

      Set<String> columnNameSet = new HashSet<>();
      UnmodifiableIterator<String> it = sortedColumns.iterator();
      while (it.hasNext() && columnNameSet.size() <
          GPDMain.userInput.getSetting().getMaxNumColumn()) {
        String columnName = it.next();
        columnNameSet.add(columnName);
      }

      filteredSchema.filterUninteresting(tables, columnNameSet);
      return filteredSchema;
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
      return null;
    }
  }

  public boolean getInterestingSchema(Workload w) {
    for (Query q : w.getQueries()) {
      try {
        currentTableSet = new HashSet<>();
        currentQuery = q;
        Statement stmt = CCJSqlParserUtil.parse(q.getContent());
        stmt.accept(this);
        if (!currentTableSet.isEmpty()) {
          interestingTableSets.add(currentTableSet);
        }
      } catch (JSQLParserException e) {
        System.out.println(q.getContent());
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  public List<Set<String>> getInterestingTableSets() {
    List<Set<String>> listOfSets = new ArrayList<>(interestingTableSets);
    Set<Set<String>> subsets = new HashSet<>();
    for (int i = 0; i < listOfSets.size(); ++i) {
      for (int j = 0; j < listOfSets.size(); ++j) {
        if (i != j) {
          Set<String> set1 = listOfSets.get(i);
          Set<String> set2 = listOfSets.get(j);
          if (set1.containsAll(set2)) {
            subsets.add(set2);
          } else if (set2.containsAll(set1)) {
            subsets.add(set1);
          }
        }
      }
    }
    listOfSets.removeAll(subsets);

    return listOfSets;
  }

  public void printSchema() {
    System.out.print("Table: ");
    for (String t : tables) {
      System.out.print(t + ", ");
    }
    System.out.println();
    System.out.print("Columns: ");
    for (String c : columns) {
      System.out.print(c + ", ");
    }
    System.out.println();
  }

  public void visitBinaryExpression(BinaryExpression binaryExpression) {
    binaryExpression.getLeftExpression().accept(this);
    binaryExpression.getRightExpression().accept(this);
  }

  @Override
  public void visit(NullValue nullValue) {

  }

  @Override
  public void visit(Function function) {

  }

  @Override
  public void visit(SignedExpression signedExpression) {

  }

  @Override
  public void visit(JdbcParameter jdbcParameter) {

  }

  @Override
  public void visit(JdbcNamedParameter jdbcNamedParameter) {

  }

  @Override
  public void visit(DoubleValue doubleValue) {

  }

  @Override
  public void visit(LongValue longValue) {

  }

  @Override
  public void visit(HexValue hexValue) {

  }

  @Override
  public void visit(DateValue dateValue) {

  }

  @Override
  public void visit(TimeValue timeValue) {

  }

  @Override
  public void visit(TimestampValue timestampValue) {

  }

  @Override
  public void visit(Parenthesis parenthesis) {
    parenthesis.getExpression().accept(this);
  }

  @Override
  public void visit(StringValue stringValue) {

  }

  @Override
  public void visit(Addition addition) {
    visitBinaryExpression(addition);
  }

  @Override
  public void visit(Division division) {
    visitBinaryExpression(division);
  }

  @Override
  public void visit(Multiplication multiplication) {
    visitBinaryExpression(multiplication);
  }

  @Override
  public void visit(Subtraction subtraction) {
    visitBinaryExpression(subtraction);
  }

  @Override
  public void visit(AndExpression andExpression) {
    visitBinaryExpression(andExpression);
  }

  @Override
  public void visit(OrExpression orExpression) {
    visitBinaryExpression(orExpression);
  }

  @Override
  public void visit(Between between) {
    between.getLeftExpression().accept(this);
    between.getBetweenExpressionStart().accept(this);
    between.getBetweenExpressionEnd().accept(this);
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    if (equalsTo.getLeftExpression() instanceof Column &&
        equalsTo.getRightExpression() instanceof Column &&
        isJoin) {
      isInteresting = true;
      visitBinaryExpression(equalsTo);
      isInteresting = false;
    }
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    isInteresting = true;
    visitBinaryExpression(greaterThan);
    isInteresting = false;
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    isInteresting = true;
    visitBinaryExpression(greaterThanEquals);
    isInteresting = false;
  }

  @Override
  public void visit(InExpression inExpression) {
    inExpression.getLeftExpression().accept(this);
    inExpression.getRightItemsList().accept(this);
  }

  @Override
  public void visit(IsNullExpression isNullExpression) {

  }

  @Override
  public void visit(LikeExpression likeExpression) {
    visitBinaryExpression(likeExpression);
  }

  @Override
  public void visit(MinorThan minorThan) {
    isInteresting = true;
    visitBinaryExpression(minorThan);
    isInteresting = false;
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    isInteresting = true;
    visitBinaryExpression(minorThanEquals);
    isInteresting = false;
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    visitBinaryExpression(notEqualsTo);
  }

  @Override
  public void visit(Column column) {
    if (isInteresting) {
      columns.add(column.getColumnName());
      currentQuery.addColumn(column.getColumnName());
    }
  }

  @Override
  public void visit(CaseExpression caseExpression) {

  }

  @Override
  public void visit(WhenClause whenClause) {

  }

  @Override
  public void visit(ExistsExpression existsExpression) {
    existsExpression.getRightExpression().accept(this);
  }

  @Override
  public void visit(AllComparisonExpression allComparisonExpression) {
    allComparisonExpression.getSubSelect().getSelectBody().accept(this);
  }

  @Override
  public void visit(AnyComparisonExpression anyComparisonExpression) {
    anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
  }

  @Override
  public void visit(Concat concat) {

  }

  @Override
  public void visit(Matches matches) {

  }

  @Override
  public void visit(BitwiseAnd bitwiseAnd) {
    visitBinaryExpression(bitwiseAnd);
  }

  @Override
  public void visit(BitwiseOr bitwiseOr) {
    visitBinaryExpression(bitwiseOr);
  }

  @Override
  public void visit(BitwiseXor bitwiseXor) {
    visitBinaryExpression(bitwiseXor);
  }

  @Override
  public void visit(CastExpression castExpression) {

  }

  @Override
  public void visit(Modulo modulo) {
    visitBinaryExpression(modulo);
  }

  @Override
  public void visit(AnalyticExpression analyticExpression) {

  }

  @Override
  public void visit(WithinGroupExpression withinGroupExpression) {

  }

  @Override
  public void visit(ExtractExpression extractExpression) {

  }

  @Override
  public void visit(IntervalExpression intervalExpression) {

  }

  @Override
  public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

  }

  @Override
  public void visit(RegExpMatchOperator regExpMatchOperator) {

  }

  @Override
  public void visit(JsonExpression jsonExpression) {

  }

  @Override
  public void visit(RegExpMySQLOperator regExpMySQLOperator) {

  }

  @Override
  public void visit(UserVariable userVariable) {

  }

  @Override
  public void visit(NumericBind numericBind) {

  }

  @Override
  public void visit(KeepExpression keepExpression) {

  }

  @Override
  public void visit(MySQLGroupConcat mySQLGroupConcat) {

  }

  @Override
  public void visit(RowConstructor rowConstructor) {

  }

  @Override
  public void visit(OracleHint oracleHint) {

  }

  @Override
  public void visit(TimeKeyExpression timeKeyExpression) {

  }

  @Override
  public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

  }

  @Override
  public void visit(ExpressionList expressionList) {
    for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
      Expression expression = (Expression) iter.next();
      expression.accept(this);
    }
  }

  @Override
  public void visit(MultiExpressionList multiExpressionList) {
    for (Iterator iter = multiExpressionList.getExprList().iterator(); iter.hasNext();) {
      ExpressionList expressionList = (ExpressionList) iter.next();
      for (Iterator iter2 = expressionList.getExpressions().iterator(); iter2.hasNext();) {
        Expression expression = (Expression) iter2.next();
        expression.accept(this);
      }
    }
  }

  @Override
  public void visit(Table table) {
    String name = table.getName();
    currentQuery.addTable(name);
    tables.add(name);
    currentTableSet.add(name);
  }

  @Override
  public void visit(SubSelect subSelect) {
    subSelect.getSelectBody().accept(this);
  }

  @Override
  public void visit(SubJoin subJoin) {
    subJoin.getLeft().accept(this);
    subJoin.getJoin().getRightItem().accept(this);
  }

  @Override
  public void visit(LateralSubSelect lateralSubSelect) {
    lateralSubSelect.getSubSelect().getSelectBody().accept(this);
  }

  @Override
  public void visit(ValuesList valuesList) {

  }

  @Override
  public void visit(TableFunction tableFunction) {

  }

  @Override
  public void visit(PlainSelect plainSelect) {
    plainSelect.getFromItem().accept(this);

    if (plainSelect.getJoins() != null) {
      for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
        Join join = (Join) joinsIt.next();
        isJoin = true;
        join.getRightItem().accept(this);
        if (join.getOnExpression() != null)
          join.getOnExpression().accept(this);
        if (plainSelect.getWhere() != null) {
          plainSelect.getWhere().accept(this);
        }
        isJoin = false;
      }
    }

    if (plainSelect.getGroupByColumnReferences() != null) {
      for (Expression expression : plainSelect.getGroupByColumnReferences())
      {
        isInteresting = true;
        expression.accept(this);
        isInteresting = false;
      }
    }

    if (plainSelect.getOrderByElements() != null) {
      for (OrderByElement element : plainSelect.getOrderByElements()) {
        isInteresting = true;
        element.accept(this);
        isInteresting = false;
      }
    }
  }

  @Override
  public void visit(SetOperationList setOperationList) {

  }

  @Override
  public void visit(WithItem withItem) {
    withItem.getSelectBody().accept(this);
  }

  @Override
  public void visit(Select select) {
    select.getSelectBody().accept(this);
  }

  @Override
  public void visit(Delete delete) {

  }

  @Override
  public void visit(Update update) {
//    if (update.getFromItem() != null) {
//      update.getFromItem().accept(this);
//    }
//
//    List<Column> columnList = update.getColumns();
//    for (Column c : columnList) {
//      columns.add(c.getColumnName());
//    }
//    List<Table> tableList = update.getTables();
//    for (Table t : tableList) {
//      tables.add(t.getName());
//    }
  }

  @Override
  public void visit(Insert insert) {
    if (insert.getSelect() != null)
      insert.getSelect().getSelectBody().accept(this);
  }

  @Override
  public void visit(Replace replace) {

  }

  @Override
  public void visit(Drop drop) {

  }

  @Override
  public void visit(Truncate truncate) {

  }

  @Override
  public void visit(CreateIndex createIndex) {

  }

  @Override
  public void visit(CreateTable createTable) {

  }

  @Override
  public void visit(CreateView createView) {

  }

  @Override
  public void visit(AlterView alterView) {

  }

  @Override
  public void visit(Alter alter) {

  }

  @Override
  public void visit(Statements statements) {

  }

  @Override
  public void visit(Execute execute) {

  }

  @Override
  public void visit(SetStatement setStatement) {

  }

  @Override
  public void visit(Merge merge) {

  }

  @Override
  public void visit(OrderByElement orderByElement) {
    isInteresting = true;
    orderByElement.getExpression().accept(this);
    isInteresting = false;
  }
}
