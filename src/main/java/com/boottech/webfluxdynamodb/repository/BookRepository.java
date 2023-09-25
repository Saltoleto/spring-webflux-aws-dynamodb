package com.boottech.webfluxdynamodb.repository;

import com.boottech.webfluxdynamodb.domain.Book;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Repository
public class BookRepository {
    public static final String TABLE_NAME = "book";
    private final DynamoDbAsyncTable<Book> bookTable;

    public BookRepository(DynamoDbEnhancedAsyncClient dynamoDbClient) {
        bookTable = dynamoDbClient
                .table(TABLE_NAME, TableSchema.fromBean(Book.class));
    }

    public Flux<Book> findAll() {
        return Flux.from(bookTable.scan().items());
    }

    public Mono<Book> findById(String id) {
        return Mono.fromFuture(bookTable.getItem(getKeyBuild(id)));
    }

    public Mono<Book> delete(String id) {
        return Mono.fromCompletionStage(bookTable.deleteItem(getKeyBuild(id)));
    }

    public Mono<Integer> count() {
        ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder().addAttributeToProject("id").build();
        AtomicInteger counter = new AtomicInteger(0);
        return Flux.from(bookTable.scan(scanEnhancedRequest))
                .doOnNext(page -> counter.getAndAdd(page.items().size()))
                .then(Mono.defer(() -> Mono.just(counter.get())));
    }

    public Mono<Book> update(Book entity) {
        var updateRequest = UpdateItemEnhancedRequest.builder(Book.class).item(entity).build();
        return Mono.fromCompletionStage(bookTable.updateItem(updateRequest));
    }

    public Mono<Book> save(Book entity){
        entity.setId(UUID.randomUUID().toString());

        var putRequest = PutItemEnhancedRequest.builder(Book.class).item(entity).build();
        return Mono.fromCompletionStage(bookTable.putItem(putRequest).thenApply(x -> entity));
    }
    private Key getKeyBuild(String id) {
        return Key.builder().partitionValue(id).build();
    }
}
