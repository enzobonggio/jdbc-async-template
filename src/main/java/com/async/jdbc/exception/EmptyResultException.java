package com.async.jdbc.exception;

import com.async.jdbc.utils.SqlUtils;
import net.sf.jsqlparser.statement.Statement;

public class EmptyResultException extends RuntimeException {

    private final String sql;

    public EmptyResultException(String sql) {
        this.sql = sql;
    }

    public Statement getSqlStatement() {
        return SqlUtils.parseSql(sql);
    }
}