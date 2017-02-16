package edu.umich.gpd.sql;

import edu.umich.gpd.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class TableSchemaExtractor implements StatementVisitor {

  private Table table;

  public TableSchemaExtractor() {
  }

  /**
   * Extract table information from CREATE TABLE statement
   * @param stmt CREATE TABLE statement
   * @return Table object
   */
  public synchronized Table extractTable(Statement stmt) {
    table = new Table();
    stmt.accept(this);
    if (table.getName().isEmpty()) {
      return null;
    } else {
      return table;
    }
  }

  @Override
  public void visit(Select select) {

  }

  @Override
  public void visit(Delete delete) {

  }

  @Override
  public void visit(Update update) {

  }

  @Override
  public void visit(Insert insert) {

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
    table.setName(createTable.getTable().getName());
    for (ColumnDefinition c : createTable.getColumnDefinitions()) {
      table.addColumn(c);
    }
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
}
