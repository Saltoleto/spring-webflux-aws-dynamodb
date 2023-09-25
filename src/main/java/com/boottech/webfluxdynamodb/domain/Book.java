package com.boottech.webfluxdynamodb.domain;

import lombok.*;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;


@DynamoDbBean
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Book {

    @Getter(onMethod=@__({@DynamoDbPartitionKey, @DynamoDbAttribute("id")}))
    private String id;

    @Getter(onMethod=@__({@DynamoDbAttribute("title")}))
    private String title;

    @Getter(onMethod=@__({@DynamoDbAttribute("page")}))
    private Integer page;

    @Getter(onMethod=@__({@DynamoDbAttribute("isbn")}))
    private String isbn;

    @Getter(onMethod=@__({@DynamoDbAttribute("description")}))
    private String description;

    @Getter(onMethod=@__({@DynamoDbAttribute("language")}))
    private String language;

    @Getter(onMethod=@__({@DynamoDbAttribute("price")}))
    private Double price;
}