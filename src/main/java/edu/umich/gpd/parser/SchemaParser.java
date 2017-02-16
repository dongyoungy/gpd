package edu.umich.gpd.parser;

import edu.umich.gpd.schema.Schema;
import edu.umich.gpd.schema.Table;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Dong Young Yoon on 2/15/17.
 */
public class SchemaParser {

  private String delimiter;

  public SchemaParser() {
    this.delimiter = "\n";
  }

  public SchemaParser(String delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * Generate list of table objects from a schema file, which contains CREATE TABLE statements
   * @param schemaFile a text file that contains CREATE TABLE statements
   * @return a schema, or null if parsing fails.
   */
  public Schema parse(File schemaFile) {
    Schema schema = new Schema();
    FileInputStream fis = null;
    try
    {
      fis = new FileInputStream(schemaFile);
      byte[] data = new byte[(int)schemaFile.length()];
      fis.read(data);
      fis.close();

      String rawCreateStatements = new String(data, "UTF-8");
      String[] createStatements = rawCreateStatements.split(this.delimiter);

      TableSchemaExtractor extractor = new TableSchemaExtractor();
      for (String createStatement : createStatements) {
        Statement stmt = CCJSqlParserUtil.parse(createStatement);
        Table table = extractor.extractTable(stmt);
        if (table != null) {
          schema.addTable(table);
        }
      }
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
      return null;
    }
    catch (IOException e)
    {
      e.printStackTrace();
      return null;
    }
    catch (JSQLParserException e)
    {
      e.printStackTrace();
      return null;
    }
    return schema;
  }
}
