package Sales;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import Sales.dto.SalesByCategory;
import Sales.dto.SalesByDay;
import Sales.dto.SalesByMonth;
import Sales.dto.Transaction;
import Sales.deserializer.JSONValueDeserializationSchema;
import static Sales.utils.JsonUtil.convertTransactionToJson;
import java.sql.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Request;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.proto.ExistsRequest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;


public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Setup
        final String topic = "sales_topic";
        final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
        final String username = "postgres";
        final String password = "postgres";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable Checkpointing
        env.enableCheckpointing(5000);

        // Kafka Source
        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("localhost:29092")
                .setTopics(topic)
                .setGroupId("flink-sales-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        transactionStream.print();

        // POSTGRES //
        // Options
        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        // Create Transactions Table
        try{
            transactionStream.addSink(JdbcSink.sink(
                """
                        CREATE TABLE IF NOT EXISTS transactions (
                            transaction_id VARCHAR(255) PRIMARY KEY,
                            product_id INTEGER,
                            product_name VARCHAR(255),
                            product_brand VARCHAR(255),
                            product_category VARCHAR(255),
                            product_price DOUBLE PRECISION,
                            product_quantity INTEGER,
                            total_amount DOUBLE PRECISION,
                            currency VARCHAR(255),
                            payment_method VARCHAR(255),
                            customer_id VARCHAR(255),
                            transaction_date TIMESTAMP
                        )
                    """,
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
            )).name("Create Transactions table");
        } catch (Exception e) {
            System.out.println("Error creating Postgres Transaction table: " + e.getMessage());
            e.printStackTrace();
        }

        // Create Sales by Category Table
        try{
            transactionStream.addSink(JdbcSink.sink(
                """
                         CREATE TABLE IF NOT EXISTS sales_by_category (
                            transaction_date DATE,
                            category VARCHAR(255),
                            total_sales DOUBLE PRECISION,
                            PRIMARY KEY (transaction_date, category)
                        )
                    """,
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
            )).name("Create Sales by Category table");
        } catch (Exception e) {
            System.out.println("Error creating Postgres Sales by Category table: " + e.getMessage());
            e.printStackTrace();
        }

        // Create Sales by Month Table
        try{
            transactionStream.addSink(JdbcSink.sink(
                """
                         CREATE TABLE IF NOT EXISTS sales_by_month (
                            year INTEGER,
                            month INTEGER,
                            total_sales DOUBLE PRECISION,
                            PRIMARY KEY (year, month)
                        )
                    """,
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
            )).name("Create Sales by Month table");
        } catch (Exception e) {
            System.out.println("Error creating Postgres Sales by Month table: " + e.getMessage());
            e.printStackTrace();
        }

        // Create Sales by Day Table
        try{
            transactionStream.addSink(JdbcSink.sink(
                """
                         CREATE TABLE IF NOT EXISTS sales_by_day (
                            transaction_date DATE PRIMARY KEY,
                            total_sales DOUBLE PRECISION
                        )
                    """,
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
            )).name("Create Sales by Day table");
        } catch (Exception e) {
            System.out.println("Error creating Postgres Sales by Day table: " + e.getMessage());
            e.printStackTrace();
        }

        // Insert into Transactions Table
        try{
            transactionStream.addSink(JdbcSink.sink(
                """
                        INSERT INTO transactions(
                            transaction_id, product_id, product_name, product_brand, product_category, product_price,
                            product_quantity, total_amount, currency, payment_method, customer_id, transaction_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (transaction_id) DO UPDATE SET
                            product_id = EXCLUDED.product_id,
                            product_name  = EXCLUDED.product_name,
                            product_brand = EXCLUDED.product_brand,
                            product_category  = EXCLUDED.product_category,
                            product_price = EXCLUDED.product_price,
                            product_quantity = EXCLUDED.product_quantity,
                            total_amount  = EXCLUDED.total_amount,
                            currency = EXCLUDED.currency,
                            payment_method = EXCLUDED.payment_method,
                            customer_id  = EXCLUDED.customer_id,
                            transaction_date = EXCLUDED.transaction_date;
                    """,
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getTransactionId());
                    preparedStatement.setInt(2, transaction.getProductId());
                    preparedStatement.setString(3, transaction.getProductName());
                    preparedStatement.setString(4, transaction.getProductBrand());
                    preparedStatement.setString(5, transaction.getProductCategory());
                    preparedStatement.setDouble(6, transaction.getProductPrice());
                    preparedStatement.setInt(7, transaction.getProductQuantity());
                    preparedStatement.setDouble(8, transaction.getTotalAmount());
                    preparedStatement.setString(9, transaction.getCurrency());
                    preparedStatement.setString(10, transaction.getPaymentMethod());
                    preparedStatement.setString(11, transaction.getCustomerId());
                    preparedStatement.setTimestamp(12, transaction.getTransactionDate());
                },
                execOptions,
                connOptions
            )).name("Insert into Transactions table");
            System.out.println("Insert into Transactions table");
        } catch (Exception e) {
            System.out.println("Error inserting Postgres Transaction table: " + e.getMessage());
            e.printStackTrace();
        }

        // Insert into Sales By Category Table
        try{
            transactionStream
                    .map(
                            transaction -> {
                                Timestamp transactionDate = transaction.getTransactionDate();
                                String category = transaction.getProductCategory();
                                double totalSales = transaction.getTotalAmount();
                                return new SalesByCategory(transactionDate, category, totalSales);
                            })
                    .keyBy(SalesByCategory::getCategory)                                                                              // Group by Category
                    .reduce(((salesByCategory, t1) -> {
                        salesByCategory.setTotalSales(salesByCategory.getTotalSales() + t1.getTotalSales());
                        return salesByCategory; }))
                    .addSink(JdbcSink.sink(
                            """
                                    INSERT INTO sales_by_category(transaction_date, category, total_sales)
                                    VALUES (?, ?, ?)
                                    ON CONFLICT (transaction_date, category) DO UPDATE SET
                                        total_sales = EXCLUDED.total_sales;
                                """,
                            (JdbcStatementBuilder<SalesByCategory>) (preparedStatement, salesbyCategory) -> {
                                preparedStatement.setTimestamp(1, salesbyCategory.getTransactionDate());
                                preparedStatement.setString(2, salesbyCategory.getCategory());
                                preparedStatement.setDouble(3, salesbyCategory.getTotalSales());
                            },
                            execOptions,
                            connOptions))
                    .name("Insert into Sales by Category table");
            System.out.println("Insert into Sales by Category table");
        } catch (Exception e) {
            System.out.println("Error inserting Postgres Sales by Category table: " + e.getMessage());
            e.printStackTrace();
        }

        // Insert into Sales By Month Table
        try{
            transactionStream
                .map(
                        transaction -> {
                            Timestamp transactionDate = transaction.getTransactionDate();
                            int year = transactionDate.toLocalDateTime().getYear();
                            int month = transactionDate.toLocalDateTime().getMonth().getValue();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesByMonth(year, month, totalSales);
                        })
                .keyBy(new KeySelector<SalesByMonth, Tuple2<Integer, Integer>>() {                                                  // Group by Year & Month
                    @Override
                    public Tuple2<Integer, Integer> getKey(SalesByMonth salesByMonth) throws Exception {
                        return Tuple2.of(salesByMonth.getYear(), salesByMonth.getMonth());
                    }},
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}))
                .reduce(((salesByMonth, t1) -> {
                    salesByMonth.setTotalSales(salesByMonth.getTotalSales() + t1.getTotalSales());
                    return salesByMonth;}))
                .addSink(JdbcSink.sink(
                        """
                                INSERT INTO sales_by_month(year, month, total_sales)
                                VALUES (?, ?, ?)
                                ON CONFLICT (year, month) DO UPDATE SET
                                    total_sales = EXCLUDED.total_sales;
                            """,
                        (JdbcStatementBuilder<SalesByMonth>) (preparedStatement, salesByMonth) -> {
                            preparedStatement.setInt(1, salesByMonth.getYear());
                            preparedStatement.setInt(2, salesByMonth.getMonth());
                            preparedStatement.setDouble(3, salesByMonth.getTotalSales());
                        },
                        execOptions,
                        connOptions))
                .name("Insert into Sales by Month table");
            System.out.println("Insert into Sales by Month table");
        } catch (Exception e) {
            System.out.println("Error inserting Postgres Sales by Month table: " + e.getMessage());
            e.printStackTrace();
        }

        // Insert into Sales By Day Table
        try{
            transactionStream
                .map(
                        transaction -> {
                            Timestamp transactionDate = transaction.getTransactionDate();
                            LocalDate date = transactionDate.toLocalDateTime().toLocalDate();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesByDay(date, totalSales);
                        })
                .keyBy(SalesByDay::getTransactionDate)                                                                              // Group by Day (Date)
                .reduce(((salesByDay, t1) -> {
                    salesByDay.setTotalSales(salesByDay.getTotalSales() + t1.getTotalSales());
                    return salesByDay;}))
                .addSink(JdbcSink.sink(
                        """
                                INSERT INTO sales_by_day(transaction_date, total_sales)
                                VALUES (?, ?)
                                ON CONFLICT (transaction_date) DO UPDATE SET
                                    total_sales = EXCLUDED.total_sales
                            """,
                        (JdbcStatementBuilder<SalesByDay>) (preparedStatement, salesByDay) -> {
                            preparedStatement.setDate(1, Date.valueOf(salesByDay.getTransactionDate()));
                            preparedStatement.setDouble(2, salesByDay.getTotalSales());
                        },
                        execOptions,
                        connOptions))
                .name("Insert into Sales by Day table");
            System.out.println("Insert into Sales by Day table");
        } catch (Exception e) {
            System.out.println("Error inserting Postgres Sales by Day table: " + e.getMessage());
            e.printStackTrace();
        }

        // ELASTICSEARCH //
        try {
            transactionStream.sinkTo(
                    new Elasticsearch7SinkBuilder<Transaction>()
                            .setHosts(new HttpHost("localhost", 9200, "http"))
                            .setEmitter((transaction, runtimeContext, requestIndexer) -> {
                                String json = convertTransactionToJson(transaction);

                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("transactions")
                                        .id(transaction.getTransactionId())
                                        .source(json, XContentType.JSON);

                                requestIndexer.add(indexRequest);
                            })
                            .build()
            ).name("Sink to Elasticsearch");
            System.out.println("Inserted data to ElasticSearch!");

            // Re-indexing Elasticsearch
            remap_Elasticsearch();
            reindex_Elasticsearch();

            env.execute("Flink Sales Stream Job");
        } catch (Exception e) {
            System.out.println("Error setting up Elasticsearch sink: " + e.getMessage());
            e.printStackTrace();
        }
    }



    // Methods to reindex Elasticsearch
    private static void remap_Elasticsearch() {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
        Request request = new Request("PUT", "/transactions_reformat");
        String jsonEntity = """
                    {
                       "mappings": {
                         "properties": {
                           "transactionDate": {
                             "type": "date",
                             "format": "yyyy-MM-dd'T'HH:mm:ss"
                           }
                         }
                       }
                     }
                """;
        request.setJsonEntity(jsonEntity);

        try {
            restClient.performRequest(request);
            System.out.println("Remapping completed!");
        } catch (IOException e) {
            // Check if the exception is a resource_already_exists_exception
            if (e.getMessage().contains("resource_already_exists_exception")) {
                System.out.println("Index already exists, skipping creation.");
            } else {
                System.out.println("Error during remapping: " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private static void reindex_Elasticsearch() {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
        Request request = new Request("POST", "/_reindex");
        String jsonEntity = """
                {
                  "source": {
                    "index": "transactions"
                  },
                  "dest": {
                    "index": "transactions_reformat"
                  },
                  "script": {
                        "source": "SimpleDateFormat formatter = new SimpleDateFormat(\\"yyyy-MM-dd'T'HH:mm:ss\\"); formatter.setTimeZone(TimeZone.getTimeZone('UTC')); ctx._source.transactionDate = formatter.format(new Date(ctx._source.transactionDate));"
                  }
                }
            """;
        request.setJsonEntity(jsonEntity);

        try {
            restClient.performRequest(request);
            System.out.println("Reindexing completed!");
        } catch (IOException e) {
            System.out.println("Error during reindexing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
