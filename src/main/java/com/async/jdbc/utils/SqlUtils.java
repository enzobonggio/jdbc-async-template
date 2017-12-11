package com.async.jdbc.utils;

import com.async.jdbc.exception.JdbcException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.Collections;

public class SqlUtils {

    private SqlUtils() {
    }

    public static String transformToCount(Statement stmt) {
        Select select = (Select) stmt;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        Function count = new Function();
        count.setName("count");
        count.setAllColumns(false);
        count.setParameters(new ExpressionList(Collections.singletonList(new LongValue("1"))));
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(count);
        item.setAlias(new Alias("total_item"));
        plainSelect.setSelectItems(Collections.singletonList(item));
        plainSelect.setGroupByColumnReferences(null);
        plainSelect.setOrderByElements(null);
        return stmt.toString();
    }

    public static Statement parseSql(String sql) {
        try {
            return CCJSqlParserUtil.parse(sql);

        } catch (JSQLParserException ex) {
            throw new JdbcException("Error trying to parse sql", sql, ex);
        }
    }

    public static String transformToPaged(Statement stmt) {
        Select select = (Select) stmt;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        plainSelect.setLimit(new Limit());
        plainSelect.setOffset(new Offset());
        return stmt.toString();
    }

    public static class Limit extends net.sf.jsqlparser.statement.select.Limit {
        @Override
        public String toString() {
            return " LIMIT :limit";
        }
    }

    public static class Offset extends net.sf.jsqlparser.statement.select.Offset {
        @Override
        public String toString() {
            return " OFFSET :offset";
        }
    }
}
