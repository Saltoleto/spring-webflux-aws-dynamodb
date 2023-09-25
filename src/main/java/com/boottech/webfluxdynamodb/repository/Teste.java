package com.boottech.webfluxdynamodb.repository;

import com.boottech.webfluxdynamodb.domain.Author;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;

public interface Teste {
    Mono<PageImpl<Author>> findAllWithPagination(Pageable pageable);
}
