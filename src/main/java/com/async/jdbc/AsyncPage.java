package com.async.jdbc;

import reactor.core.publisher.Flux;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface AsyncPage<T> {

    Integer getCurrentPage();

    Flux<T> getContent();

    Long getTotalElements();

    Integer getNumberOfElements();

    Integer getTotalPages();

    default AsyncPage<T> getNextPage() {
        throw new NotImplementedException();
    }

}
