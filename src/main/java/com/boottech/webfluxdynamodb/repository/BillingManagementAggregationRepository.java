package com.boottech.webfluxdynamodb.repository;

import com.boottech.webfluxdynamodb.domain.Author;
import com.boottech.webfluxdynamodb.domain.BillingManagementAggregation;
import com.boottech.webfluxdynamodb.domain.SummaryDTO;
import org.springframework.stereotype.Repository;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Repository
public class BillingManagementAggregationRepository {
    public static final String TABLE_NAME = "billing-management-aggregation";
    private final DynamoDbAsyncTable<BillingManagementAggregation> dynamoDbAsyncTable;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public BillingManagementAggregationRepository(DynamoDbEnhancedAsyncClient dynamoDbClient, DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncTable = dynamoDbClient.table(TABLE_NAME, TableSchema.fromBean(BillingManagementAggregation.class));
        ;
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    }


    public Mono<SummaryDTO> getVencidos() {

        Map<String, AttributeValue> vencidosExpressionValues = new HashMap<>();
        vencidosExpressionValues.put(":val1", AttributeValue.fromS("2023-10-30"));
        vencidosExpressionValues.put(":val2", AttributeValue.fromS("ABE"));

        Expression vencidosExpression = Expression.builder()
                .expression("data_vencimento <:val1 AND codigo_status_emissao = :val2")
                .expressionValues(vencidosExpressionValues)
                .build();

        QueryEnhancedRequest vencidosQueryEnhancedRequest = QueryEnhancedRequest.builder().queryConditional(QueryConditional.keyEqualTo(
                        Key.builder().partitionValue("ATLAS#12345").build()
                )).filterExpression(vencidosExpression)
                .build();

        Map<String, AttributeValue> pagosExpressionValues = new HashMap<>();
        pagosExpressionValues.put(":val1", AttributeValue.fromS("PAG"));


        Expression pagosExpression = Expression.builder()
                .expression("codigo_status_emissao = :val1")
                .expressionValues(pagosExpressionValues)
                .build();

        QueryEnhancedRequest pagosQueryEnhancedRequest = QueryEnhancedRequest.builder().queryConditional(QueryConditional.keyEqualTo(
                        Key.builder().partitionValue("ATLAS#12345").build()
                )).filterExpression(pagosExpression)
                .build();

        Map<String, AttributeValue> receberExpressionValues = new HashMap<>();
        receberExpressionValues.put(":val1", AttributeValue.fromS("2023-10-30"));
        receberExpressionValues.put(":val2", AttributeValue.fromS("ABE"));


        Expression receberExpression = Expression.builder()
                .expression("data_vencimento >= :val1 AND codigo_status_emissao = :val2")
                .expressionValues(receberExpressionValues)
                .build();

        QueryEnhancedRequest receberQueryEnhancedRequest = QueryEnhancedRequest.builder().queryConditional(QueryConditional.keyEqualTo(
                        Key.builder().partitionValue("ATLAS#12345").build()
                )).filterExpression(receberExpression)
                .build();

        PagePublisher<BillingManagementAggregation> accountsVencidas = dynamoDbAsyncTable.query(vencidosQueryEnhancedRequest);

        PagePublisher<BillingManagementAggregation> accountsPagas = dynamoDbAsyncTable.query(pagosQueryEnhancedRequest);

        PagePublisher<BillingManagementAggregation> receber = dynamoDbAsyncTable.query(receberQueryEnhancedRequest);


        Mono<BigDecimal> pagasMono = Mono.from(accountsPagas)
                .map(scanResponse -> scanResponse.items().stream().map(BillingManagementAggregation::getValorDeclarado)
                        .reduce(BigDecimal.ZERO, BigDecimal::add));

        Mono<BigDecimal> vencidasMono = Mono.from(accountsVencidas)
                .map(scanResponse -> scanResponse.items().stream().map(BillingManagementAggregation::getValorDeclarado)
                        .reduce(BigDecimal.ZERO, BigDecimal::add));

        Mono<BigDecimal> receberMono = Mono.from(receber)
                .map(scanResponse -> scanResponse.items().stream().map(BillingManagementAggregation::getValorDeclarado)
                        .reduce(BigDecimal.ZERO, BigDecimal::add));

        return Mono.zip(pagasMono, vencidasMono, receberMono)
                .map(tuple -> SummaryDTO.builder().totalRecebido(tuple.getT1())
                        .totalVencido(tuple.getT2())
                        .totalAVencer(tuple.getT3()).build());

    }


}
