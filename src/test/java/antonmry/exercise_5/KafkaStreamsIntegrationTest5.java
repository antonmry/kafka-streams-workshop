package antonmry.exercise_5;

import antonmry.clients.producer.MockDataProducer;
import antonmry.model.Purchase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class KafkaStreamsIntegrationTest5 {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private static KafkaStreamsApp5 kafkaStreamsApp;
    private static Properties producerConfig;
    private static Properties consumerConfig;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String PURCHASES_TOPIC = "purchases";
    private static final String PATTERNS_TOPIC = "patterns";
    private static final String PURCHASES_TABLE_TOPIC = "customer_detection";

    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void setUpAll() throws Exception {
        EMBEDDED_KAFKA.createTopic(TRANSACTIONS_TOPIC);
        EMBEDDED_KAFKA.createTopic(PURCHASES_TOPIC);
        EMBEDDED_KAFKA.createTopic(PATTERNS_TOPIC);

        Properties properties = StreamsTestUtils.getStreamsConfig("integrationTest",
                EMBEDDED_KAFKA.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        kafkaStreamsApp = new KafkaStreamsApp5(properties);
        kafkaStreamsApp.start();

        producerConfig = TestUtils.producerConfig(EMBEDDED_KAFKA.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class);

        consumerConfig = TestUtils.consumerConfig(EMBEDDED_KAFKA.bootstrapServers(), "test",
                StringDeserializer.class,
                StringDeserializer.class);

        MockDataProducer.producePurchaseData(producerConfig);
    }

    @AfterClass
    public static void tearDown() {
        kafkaStreamsApp.stop();
    }

    @Test
    public void testQueryableKTable() throws Exception {

        MockDataProducer.producePurchaseData(producerConfig);

        int expectedNumberOfRecords = 100;
        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TOPIC,
                        expectedNumberOfRecords),
                Purchase.class);

        System.out.println(PURCHASES_TOPIC + " received: " + actualValues);
        System.out.println(PURCHASES_TOPIC + " count: " + actualValues.stream().count());

        int tableNumberOfRecords = 20;
        List<Purchase> tableValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TABLE_TOPIC,
                        tableNumberOfRecords),
                Purchase.class);

        System.out.println(PURCHASES_TABLE_TOPIC + " received: " + tableValues);
        System.out.println(PURCHASES_TABLE_TOPIC + " count: " + tableValues.stream().count());

        // We need to give time to the state store to be available
        Thread.sleep(20000);

        KeyValueIterator<String, Purchase> range = kafkaStreamsApp.getCustomersTableRecords();
        int total = 0;
        System.out.print("customers event store keys: ");
        while (range.hasNext()) {
            KeyValue<String, Purchase> next = range.next();
            System.out.print(next.key + ", ");
            total++;
        }
        assertThat(total, is(greaterThan(0)));
    }

    @Test
    public void printTopology() throws Exception {
        System.out.println(kafkaStreamsApp.getTopology());
    }
}
