package com.async.jdbc.impl;

import com.async.jdbc.AsyncPage;
import reactor.core.publisher.Flux;

public class JdbcAsyncPage<T> implements AsyncPage<T> {
    private final Flux<T> content;
    private final Integer numberOfElements;
    private final Long totalElements;
    private final Integer currentPage;
    private final Integer totalPages;

    JdbcAsyncPage(Flux<T> content, Integer numberOfElements, Long totalElements, Integer currentPage, Integer totalPages) {
        this.content = content;
        this.numberOfElements = numberOfElements;
        this.totalElements = totalElements;
        this.currentPage = currentPage;
        this.totalPages = totalPages;
    }

    static <T> JdbcAsyncPageBuilder<T> builder() {
        return new JdbcAsyncPageBuilder<>();
    }

    static PageInfo pageInfoOf(Long count, Integer page, Integer limit) {
        final Integer totalPages = (int) Math.ceil(count / (double) page);
        return new PageInfo(limit, count, page, totalPages);
    }

    @Override
    public Integer getCurrentPage() {
        return currentPage;
    }

    @Override
    public Flux<T> getContent() {
        return content;
    }

    @Override
    public Long getTotalElements() {
        return totalElements;
    }

    @Override
    public Integer getNumberOfElements() {
        return numberOfElements;
    }

    @Override
    public Integer getTotalPages() {
        return totalPages;
    }

    static class PageInfo {
        private final Integer numberOfElements;
        private final Long totalElements;
        private final Integer currentPage;
        private final Integer totalPages;

        PageInfo(Integer numberOfElements, Long totalElements, Integer currentPage, Integer totalPages) {
            this.numberOfElements = numberOfElements;
            this.totalElements = totalElements;
            this.currentPage = currentPage;
            this.totalPages = totalPages;
        }

        Integer getOffset() {
            return currentPage * numberOfElements;
        }

        Integer getLimit() {
            return numberOfElements;
        }
    }

    static class JdbcAsyncPageBuilder<T> {
        private Flux<T> content;
        private Integer numberOfElements;
        private Long totalElements;
        private Integer currentPage;
        private Integer totalPages;

        private JdbcAsyncPageBuilder() {
        }

        JdbcAsyncPageBuilder<T> pagedInformation(PageInfo pageInfo) {
            return this
                    .totalElements(pageInfo.totalElements)
                    .currentPage(pageInfo.currentPage)
                    .numberOfElements(pageInfo.numberOfElements)
                    .totalPages(pageInfo.totalPages);
        }

        JdbcAsyncPageBuilder<T> totalElements(Long totalElements) {
            this.totalElements = totalElements;
            return this;
        }

        JdbcAsyncPageBuilder<T> totalPages(Integer totalPages) {
            this.totalPages = totalPages;
            return this;
        }

        JdbcAsyncPageBuilder<T> content(Flux<T> content) {
            this.content = content;
            return this;
        }

        JdbcAsyncPageBuilder<T> numberOfElements(Integer numberOfElements) {
            this.numberOfElements = numberOfElements;
            return this;
        }

        JdbcAsyncPageBuilder<T> currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        JdbcAsyncPage<T> build() {
            return new JdbcAsyncPage<>(content, numberOfElements, totalElements, currentPage, totalPages);
        }
    }
}
