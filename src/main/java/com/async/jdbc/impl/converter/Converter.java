package com.async.jdbc.impl.converter;

import com.async.jdbc.exception.JdbcException;
import com.github.pgasync.Row;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.PgRow;
import com.google.common.base.CaseFormat;
import org.javatuples.Pair;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Converter {

    private static final boolean TRUE = true;
    private static final String SPACE = " ";

    private final Map<Class<?>, Function<PgRow, Object>> convertionStrategies;

    public Converter() {
        convertionStrategies = new HashMap<>();
        convertionStrategies.put(Map.class, this::convertToMap);
        convertionStrategies.put(Integer.class, pgRow -> pgRow.getInt(0));
        convertionStrategies.put(Long.class, pgRow -> pgRow.getLong(0));
        convertionStrategies.put(Boolean.class, pgRow -> pgRow.getBoolean(0));
        convertionStrategies.put(Row.class, pgRow -> pgRow);
    }

    private Map<String, Object> convertToMap(PgRow pgRow) {
        return pgRow
                .getColumns().keySet()
                .parallelStream()
                .map(key -> Pair.with(
                        key, pgRow.get(key)))
                .filter(pair -> Objects.nonNull(pair.getValue1()))
                .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));

    }

    @SuppressWarnings("unchecked")
    public <T> Function<PgRow, T> getStrategy(Class<T> elementType) {
        return Optional
                .ofNullable((Function<PgRow, T>) convertionStrategies.get(elementType))
                .orElse(pgRow -> defaultStrategy(pgRow, elementType));
    }

    public Pair<String, Object[]> convertParams(String sql, Map<String, Object> params) {
        String[] words = sql.split(SPACE);
        List<Object> objects = new ArrayList<>();
        final List<String> sb = new ArrayList<>(words.length);
        int pos = 1;

        for (String word : words) {
            if (word.matches(":[A-z]+")) {
                String paramName = word.substring(1);
                sb.add("$" + pos);
                ++pos;
                if (params.containsKey(paramName)) {
                    objects.add(params.get(paramName));
                } else {
                    throw new JdbcException(String.format("Cannot find param name %s", paramName), sql);
                }
            } else {
                sb.add(word);
            }
        }
        return Pair.with(String.join(SPACE, sb), objects.toArray());
    }

    private <T> T defaultStrategy(PgRow pgRow, Class<T> elementType) {
        return StreamSupport
                .stream(Spliterators.spliterator(
                        Arrays.asList(elementType.getDeclaredFields()),
                        elementType.getDeclaredFields().length), TRUE)
                .map(field -> Pair.with(field, getFieldNameWithLowerCase(field.getName())))
                .filter(fieldPair -> pgRow.getColumns().containsKey(fieldPair.getValue1()))
                .map(fieldPair -> fieldPair.setAt1(pgRow.get(fieldPair.getValue1())))
                .collect(Collector.of(
                        () -> newIntance(elementType),
                        this::setField,
                        this::mergeObjects));
    }

    private String getFieldNameWithLowerCase(String name) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name);
    }

    private <T> T setField(T obj, Pair<Field, Object> fieldPair) {
        Field field = fieldPair.getValue0();
        field.setAccessible(true);
        Object value = fieldPair.getValue1();
        try {
            field.set(obj, value);
            return obj;
        } catch (IllegalAccessException | IllegalArgumentException ex) {
            throw new JdbcException(String.format("Error when try to set value on VO object field: %s", field.getName()), ex);
        }
    }

    private <T> T newIntance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new SqlException("Error when try to instance VO object", ex);
        }
    }

    private <T> T mergeObjects(T first, T second) {
        Stream.of(second.getClass().getDeclaredFields())
                .peek(field -> field.setAccessible(true))
                .map(field -> {
                    try {
                        return Pair.with(field, field.get(second));
                    } catch (IllegalAccessException ex) {
                        throw new SqlException("Error when try to get field on merge", ex);
                    }
                })
                .filter(pairField -> Objects.nonNull(pairField.getValue1()))
                .forEach(pairField -> {
                    try {
                        pairField.getValue0().set(first, pairField.getValue1());
                    } catch (IllegalAccessException ex) {
                        throw new SqlException("Error when try to set field on merge", ex);
                    }
                });
        return first;
    }
}
