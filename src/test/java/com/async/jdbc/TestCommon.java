package com.async.jdbc;

import com.async.jdbc.impl.converter.Converter;
import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ConnectionPoolBuilder;

public class TestCommon {

    public static ConnectionPool getConnectionPool() {
        return new ConnectionPoolBuilder()
                .poolSize(5)
                .database("dbcat")
                .hostname("localhost")
                .password("password")
                .username("postgres")
                .port(5432)
                .build();
    }

    public static Converter getConvertStrategy() {
        return new Converter();
    }
}
