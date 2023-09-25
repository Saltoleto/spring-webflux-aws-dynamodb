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
public class Author {

    @Getter(onMethod=@__({@DynamoDbPartitionKey, @DynamoDbAttribute("id")}))
    private String id;

    @Getter(onMethod=@__({@DynamoDbAttribute("lastname")}))
    private String lastname;

    @Getter(onMethod=@__({@DynamoDbAttribute("middleName")}))
    private String middleName;

    @Getter(onMethod=@__({@DynamoDbAttribute("firstname")}))
    private String firstname;
}
