import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional
import java.time.LocalDate

    QueryConditional queryConditional = QueryConditional.between(
       Key.builder()
               .partitionValue("suaChaveDeParticao")
               .build(),
       Key.builder()
               .partitionValue("suaChaveDeParticao")
               .sortValue(startDate) // Data de início
               .build(),
       Key.builder()
               .partitionValue("suaChaveDeParticao")
               .sortValue(endDate) // Data de término
               .build()
);


// Crie um objeto DynamoDbEnhancedAsyncClient, substituindo com suas configurações
val enhancedAsyncClient = DynamoDbEnhancedAsyncClient.builder().build()

// Especifique as datas de início e término que você deseja consultar
val startDate = LocalDate.parse("2023-09-12")
val endDate = LocalDate.parse("2023-10-12")

// Especifique o valor do índice que você deseja consultar
val indexValue = "SeuValorDeIndiceAqui"

// Crie uma condição de consulta para a faixa de datas e o valor do índice
val queryConditional = QueryConditional
    .between(
        QueryConditional.sortKeyBind("data_vencimento"),
        startDate,
        endDate
    )
    .and(QueryConditional.equalTo(QueryConditional.indexKeyBind("seu_indice"), indexValue))

// Execute a consulta na tabela com o índice GSI "Fernando"
val table = enhancedAsyncClient.table("NomeDaSuaTabela", YourItem::class.java)
val gsi = table.index("Fernando") // Substitua "Fernando" pelo nome do seu GSI
val items = gsi.query(queryConditional).items().toList()

// Agora 'items' contém os itens que atendem à sua condição de consulta usando o GSI e o valor do índice
