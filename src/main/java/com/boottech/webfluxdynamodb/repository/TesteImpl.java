package com.boottech.webfluxdynamodb.repository;

import com.boottech.webfluxdynamodb.domain.Author;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Repository
public class TesteImpl implements Teste {

    private final DynamoDbEnhancedAsyncClient enhancedAsyncClient;
    private final DynamoDbAsyncTable<Author> userTable;

    private final DynamoDbAsyncClient dynamoDbAsyncClient;


    @Autowired
    public TesteImpl(DynamoDbEnhancedAsyncClient enhancedAsyncClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.enhancedAsyncClient = enhancedAsyncClient;
        this.userTable = enhancedAsyncClient.table("author", TableSchema.fromBean(Author.class));
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    }

    @Override
    public Mono<PageImpl<Author>> findAllWithPagination(Pageable pageable) {
        int pageSize = pageable.getPageSize();

        ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder()
                .limit(pageSize).build();

        PagePublisher<Author> scan = userTable.scan(scanEnhancedRequest);

        return Mono.from(scan).map(scanResponse -> {
            List<Author> authors = new ArrayList<>();
            scanResponse.items().forEach(item -> {
                authors.add(item);
            });

            return new PageImpl<>(authors, pageable, authors.size());
        });


    }

    private Author mapDynamoDBItemToUser(Map<String, AttributeValue> item) {
        Author author = new Author();
        author.setFirstname(item.get("firstname").s());
        return author;
    }
}
