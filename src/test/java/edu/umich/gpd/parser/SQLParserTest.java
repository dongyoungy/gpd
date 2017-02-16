package edu.umich.gpd.parser;

import edu.umich.gpd.schema.Table;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Created by Dong Young Yoon on 2/13/17.
 */
public class SQLParserTest {

  public static void main(String[] args) {
    try
    {
      Statement stmt = CCJSqlParserUtil.parse("SELECT c_discount, c_last, c_credit, w_tax" +
              " FROM customer, warehouse" +
          " WHERE w_id = 1 AND c_w_id = w_id AND c_d_id = d_id AND c_id = c_id;");

      String sql = "SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY,\n" +
          " SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,\n" +
          " SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS SUM_CHARGE, AVG(L_QUANTITY) AS AVG_QTY,\n" +
          " AVG(L_EXTENDEDPRICE) AS AVG_PRICE, AVG(L_DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER\n" +
          "FROM LINEITEM\n" +
          "WHERE L_SHIPDATE <= dateadd(dd, -90, cast('1998-12-01' as date))\n" +
          "GROUP BY L_RETURNFLAG, L_LINESTATUS\n" +
          "ORDER BY L_RETURNFLAG,L_LINESTATUS";

      String sql2 = "SELECT\n" +
          "    sum(l_extendedprice* (1 - l_discount)) as revenue\n" +
          "FROM\n" +
          "    lineitem,\n" +
          "    part\n" +
          "WHERE\n" +
          "    (\n" +
          "        p_partkey = l_partkey\n" +
          "        AND p_brand = 'Brand#12'\n" +
          "        AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n" +
          "        AND l_quantity >= 1 AND l_quantity <= 1 + 10\n" +
          "        AND p_size between 1 AND 5\n" +
          "        AND l_shipmode in ('AIR', 'AIR REG')\n" +
          "        AND l_shipinstruct = 'DELIVER IN PERSON'\n" +
          "    )\n" +
          "    OR\n" +
          "    (\n" +
          "        p_partkey = l_partkey\n" +
          "        AND p_brand = 'Brand#23'\n" +
          "        AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
          "        AND l_quantity >= 10 AND l_quantity <= 10 + 10\n" +
          "        AND p_size between 1 AND 10\n" +
          "        AND l_shipmode in ('AIR', 'AIR REG')\n" +
          "        AND l_shipinstruct = 'DELIVER IN PERSON'\n" +
          "    )\n" +
          "    OR\n" +
          "    (\n" +
          "        p_partkey = l_partkey\n" +
          "        AND p_brand = 'Brand#34'\n" +
          "        AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
          "        AND l_quantity >= 20 AND l_quantity <= 20 + 10\n" +
          "        AND p_size between 1 AND 15\n" +
          "        AND l_shipmode in ('AIR', 'AIR REG')\n" +
          "        AND l_shipinstruct = 'DELIVER IN PERSON'\n" +
          "    );";

      String create1 = "CREATE TABLE CUSTOMER (C_ID           NUMBER(5, 0), \n" +
          "                       C_D_ID         NUMBER(2, 0), \n" +
          "                       C_W_ID         NUMBER(4, 0), \n" +
          "                       C_FIRST        VARCHAR2(16), \n" +
          "                       C_MIDDLE       CHAR(2), \n" +
          "                       C_LAST         VARCHAR2(16), \n" +
          "                       C_STREET_1     VARCHAR2(20), \n" +
          "                       C_STREET_2     VARCHAR2(20), \n" +
          "                       C_CITY         VARCHAR2(20), \n" +
          "                       C_STATE        CHAR(2), \n" +
          "                       C_ZIP          CHAR(9), \n" +
          "                       C_PHONE        CHAR(16), \n" +
          "                       C_SINCE        DATE, \n" +
          "                       C_CREDIT       CHAR(2), \n" +
          "                       C_CREDIT_LIM   NUMBER(12, 2), \n" +
          "                       C_DISCOUNT     NUMBER(4, 4), \n" +
          "                       C_BALANCE      NUMBER(12, 2), \n" +
          "                       C_YTD_PAYMENT  NUMBER(12, 2), \n" +
          "                       C_PAYMENT_CNT  NUMBER(8, 0), \n" +
          "                       C_DELIVERY_CNT NUMBER(8, 0), \n" +
          "                       C_DATA         VARCHAR2(500)) \n";

      Statement stmt2 = CCJSqlParserUtil.parse(sql2);
      Statement customerTableStmt = CCJSqlParserUtil.parse(create1);

      InterestingSchemaFinder finder = new InterestingSchemaFinder();
      stmt2.accept(finder);

      TableSchemaExtractor extractor = new TableSchemaExtractor();
      Table customerTable = extractor.extractTable(customerTableStmt);

      finder.printSchema();
      System.out.println(customerTable.toString());
    }
    catch (JSQLParserException e)
    {
      e.printStackTrace();
    }
  }
}
