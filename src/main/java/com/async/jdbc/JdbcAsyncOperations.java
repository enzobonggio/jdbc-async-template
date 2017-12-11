package com.async.jdbc;

import org.intellij.lang.annotations.Language;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface JdbcAsyncOperations {

    <T> Flux<T> queryForObjects(@Language("SQL") String sql, Map<String, Object> params, Class<T> elementType);

    <T> Mono<T> queryForObject(@Language("SQL") String sql, Map<String, Object> params, Class<T> elementType);

    <T> Mono<AsyncPage<T>> queryForPagedObjects(@Language("SQL") String sql, Map<String, Object> params, Class<T> elementType, Integer page, Integer limit);

}
