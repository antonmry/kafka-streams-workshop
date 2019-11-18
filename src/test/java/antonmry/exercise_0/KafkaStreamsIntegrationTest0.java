package antonmry.exercise_0;

import antonmry.clients.producer.MockDataProducer;
import antonmry.model.Purchase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.*;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertThat;

public class KafkaStreamsIntegrationTest0 {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private KafkaStreamsApp0 kafkaStreamsApp;
    private Properties producerConfig;
    private Properties consumerConfig;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String PURCHASES_TOPIC = "purchases";

    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void setUpAll() throws Exception {
        EMBEDDED_KAFKA.createTopic(TRANSACTIONS_TOPIC);
        EMBEDDED_KAFKA.createTopic(PURCHASES_TOPIC);
    }

    @Before
    public void setUp() {

        Properties properties = StreamsTestUtils.getStreamsConfig("integrationTest",
                EMBEDDED_KAFKA.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        kafkaStreamsApp = new KafkaStreamsApp0(properties);
        kafkaStreamsApp.start();

        producerConfig = TestUtils.producerConfig(EMBEDDED_KAFKA.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class);

        consumerConfig = TestUtils.consumerConfig(EMBEDDED_KAFKA.bootstrapServers(), "test",
                StringDeserializer.class,
                StringDeserializer.class);
    }

    @After
    public void tearDown() {
        kafkaStreamsApp.stop();
    }

    @Test
    public void maskCreditCards() throws Exception {

        MockDataProducer.producePurchaseData(producerConfig);

        int expectedNumberOfRecords = 100;
        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TOPIC,
                        expectedNumberOfRecords),
                Purchase.class);

        System.out.println("Received: " + actualValues);

        actualValues
                .stream()
                .forEach(v -> assertThat(
                        v.getCreditCardNumber(),
                        matchesPattern("xxxx-xxxx-xxxx-\\d\\d\\d\\d")
                ));

    }

    @Test
    public void printTopology() throws Exception {
        System.out.println(kafkaStreamsApp.getTopology());
    }

}
