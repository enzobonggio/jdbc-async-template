package com.async.jdbc.exception;

import com.async.jdbc.utils.SqlUtils;
import com.github.pgasync.SqlException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

public class JdbcException extends RuntimeException {
    private final String code;
    private final String sql;

    public JdbcException(SqlException ex) {
        super(ex.getMessage(), ex.getCause());
        this.code = ex.getCode();
        this.sql = null;
    }

    public JdbcException(String message, String sql, JSQLParserException ex) {
        super(message, ex);
        this.code = null;
        this.sql = sql;
    }

    public JdbcException(String message, String sql) {
        super(message);
        this.code = null;
        this.sql = sql;
    }

    public JdbcException(String message, Exception ex) {
        super(message, ex);
        this.code = null;
        this.sql = null;
    }

    public Statement getSqlStatement() {
        return SqlUtils.parseSql(sql);
    }

    public String getCode() {
        return code;
    }
}
