package com.async.jdbc.impl;

import com.async.jdbc.AsyncPage;
import com.async.jdbc.JdbcAsyncOperations;
import com.async.jdbc.exception.EmptyResultException;
import com.async.jdbc.exception.JdbcException;
import com.async.jdbc.impl.converter.Converter;
import com.async.jdbc.utils.SqlUtils;
import com.github.pgasync.ConnectionPool;
import com.github.pgasync.Row;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.PgRow;
import com.sun.tools.javac.util.Assert;
import org.javatuples.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class JdbcAsyncTemplate implements JdbcAsyncOperations {

    private final ConnectionPool pool;
    private final Converter converter;


    public JdbcAsyncTemplate(ConnectionPool pool, Converter converter) {
        this.pool = pool;
        this.converter = converter;
    }

    @Override
    public <T> Flux<T> queryForObjects(String sql, Map<String, Object> params, Class<T> elementType) {
        final Pair<String, Object[]> sqlParams = converter.convertParams(sql, params);
        return Flux.<T>create(sink -> pool
                .queryRows(sqlParams.getValue0(), sqlParams.getValue1())
                .subscribe(
                        next -> sink.next(convertRowToClass(next, elementType)),
                        sink::error,
                        sink::complete))
                .onErrorMap(SqlException.class, JdbcException::new);
    }

    @Override
    public <T> Mono<T> queryForObject(String sql, Map<String, Object> params, Class<T> elementType) {
        final Pair<String, Object[]> sqlParams = converter.convertParams(sql, params);
        return Mono.<T>create(sink -> pool
                .queryRows(sqlParams.getValue0(), sqlParams.getValue1())
                .subscribe(
                        result -> sink.success(convertRowToClass(result, elementType)),
                        sink::error,
                        sink::success))
                .switchIfEmpty(Mono.error(new EmptyResultException(sql)))
                .onErrorMap(SqlException.class, JdbcException::new);
    }

    @Override
    public <T> Mono<AsyncPage<T>> queryForPagedObjects(String sql, Map<String, Object> params, Class<T> elementType, Integer page, Integer limit) {
        Assert.checkNonNull(params);
        return countQuery(sql)
                .flatMap(s -> queryForObject(s, new HashMap<>(), Long.class))
                .map(count -> JdbcAsyncPage.pageInfoOf(count, page, limit))
                .map(pageInfo -> {
                    params.put("offset", pageInfo.getOffset());
                    params.put("limit", pageInfo.getLimit());
                    return JdbcAsyncPage
                            .<T>builder()
                            .pagedInformation(pageInfo);
                })
                .flatMap(builder -> offsetLimitQuery(sql)
                        .map(s -> builder.content(queryForObjects(s, params, elementType)))
                        .map(JdbcAsyncPage.JdbcAsyncPageBuilder::build));
    }

    private Mono<String> countQuery(String sql) {
        return Mono.just(sql)
                .map(SqlUtils::parseSql)
                .map(SqlUtils::transformToCount);
    }

    private Mono<String> offsetLimitQuery(String sql) {
        return Mono.just(sql)
                .map(SqlUtils::parseSql)
                .map(SqlUtils::transformToPaged);
    }

    private <T> T convertRowToClass(Row row, Class<T> elementType) {
        final PgRow pgRow = ((PgRow) row);
        return converter.getStrategy(elementType).apply(pgRow);
    }
}
