package com.boottech.webfluxdynamodb.repository;

import com.boottech.webfluxdynamodb.domain.Author;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Repository
public class AuthorRepository {
    public static final String TABLE_NAME = "author";
    private final DynamoDbAsyncTable<Author> authorTable;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public AuthorRepository(DynamoDbEnhancedAsyncClient dynamoDbClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        authorTable = dynamoDbClient
                .table(TABLE_NAME, TableSchema.fromBean(Author.class));
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;

    }



    public Mono<PageImpl<Author>> findAll2(Pageable pageable) {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(TABLE_NAME)
                .limit(1)  // Limit the number of items per page
                .build();


        if (pageable.getPageNumber() > 1) {
            Map<String, AttributeValue> exclusiveStartKey = Map.of("id", AttributeValue.builder().s("lastEvaluatedKey").build());
            scanRequest = scanRequest.toBuilder().build();
        }
        CompletableFuture<ScanResponse> scanResponseFuture = dynamoDbAsyncClient.scan(scanRequest);

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":val1",  AttributeValue.fromS("Fernando"));


        Expression expression = Expression.builder()
                .expression("firstname = :val1")
                .expressionValues(expressionValues)
                .build();

        ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder()
                .addAttributeToProject("id")
                .addAttributeToProject("firstname")
                .addAttributeToProject("lastname")
                .addAttributeToProject("middleName")
                .filterExpression(expression)
                .build();

        PagePublisher<Author> scan = authorTable.scan(scanEnhancedRequest);


        return Mono.fromFuture(() -> scanResponseFuture)
                .map(scanResponse -> {
                    List<Author> authors = new ArrayList<>();
                    scanResponse.items().forEach(item -> {
                        Author author = mapDynamoDBItemToUser(item);
                        authors.add(author);
                    });

                    String lastEvaluatedKey = scanResponse.lastEvaluatedKey() != null ?
                            scanResponse.lastEvaluatedKey().get("id").s() : null;

                    return new PageImpl<>(authors, pageable, authors.size());
                });

    }

    public Mono<Integer> count() {
        ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder().addAttributeToProject("id").build();
        AtomicInteger counter = new AtomicInteger(0);
        return Flux.from(authorTable.scan(scanEnhancedRequest))
                .doOnNext(page -> counter.getAndAdd(page.items().size()))
                .then(Mono.defer(() -> Mono.just(counter.get())));
    }


    private Author mapDynamoDBItemToUser(Map<String, AttributeValue> item) {
        Author author = new Author();
        author.setFirstname(item.get("firstname").s());
        return author;
    }

    public Flux<Author> findAll() {
        return Flux.from(authorTable.scan().items());
    }

    public Mono<Author> findById(String id) {
        return Mono.fromFuture(authorTable.getItem(getKeyBuild(id)));
    }

    public Mono<Author> delete(String id) {
        return Mono.fromCompletionStage(authorTable.deleteItem(getKeyBuild(id)));
    }

    public Mono<Author> update(Author entity) {
        var updateRequest = UpdateItemEnhancedRequest.builder(Author.class).item(entity).build();
        return Mono.fromCompletionStage(authorTable.updateItem(updateRequest));
    }

    public Mono<Author> save(Author entity) {
        entity.setId(UUID.randomUUID().toString());

        var putRequest = PutItemEnhancedRequest.builder(Author.class).item(entity).build();
        return Mono.fromCompletionStage(authorTable.putItem(putRequest).thenApply(x -> entity));
    }

    private Key getKeyBuild(String id) {
        return Key.builder().partitionValue(id).build();
    }
}
