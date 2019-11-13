package antonmry.solution;

import antonmry.clients.producer.MockDataProducer;
import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class KafkaStreamsIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private static KafkaStreamsApp kafkaStreamsApp;
    private static Properties producerConfig;
    private static Properties consumerConfig;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String PURCHASES_TOPIC = "purchases";
    private static final String PATTERNS_TOPIC = "patterns";
    private static final String PURCHASES_TABLE_TOPIC = "customer_detection";
    private static final String REWARDS_TOPIC = "rewards";
    private static final String SHOES_TOPIC = "shoes";
    private static final String FRAGRANCES_TOPIC = "fragrances";
    private static final String SHOES_AND_FRAGANCES_TOPIC = "shoesAndFragrancesAlerts";

    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void setUpAll() throws Exception {
        EMBEDDED_KAFKA.createTopic(TRANSACTIONS_TOPIC);
        EMBEDDED_KAFKA.createTopic(PURCHASES_TOPIC);
        EMBEDDED_KAFKA.createTopic(PATTERNS_TOPIC);
        EMBEDDED_KAFKA.createTopic(REWARDS_TOPIC);
        EMBEDDED_KAFKA.createTopic(PURCHASES_TABLE_TOPIC);
        EMBEDDED_KAFKA.createTopic(SHOES_TOPIC);
        EMBEDDED_KAFKA.createTopic(FRAGRANCES_TOPIC);
        EMBEDDED_KAFKA.createTopic(SHOES_AND_FRAGANCES_TOPIC);

        Properties properties = StreamsTestUtils.getStreamsConfig("integrationTest",
                EMBEDDED_KAFKA.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        kafkaStreamsApp = new KafkaStreamsApp(properties);
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
    public void testPurchasePatterns() throws Exception {

        int expectedNumberOfRecords = 100;
        List<PurchasePattern> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PATTERNS_TOPIC,
                        expectedNumberOfRecords),
                PurchasePattern.class);

        System.out.println(PATTERNS_TOPIC + " received: " + actualValues);

        actualValues.stream().forEach(v -> assertThat(
                v.getZipCode(), not(isEmptyString())));
        actualValues.stream().forEach(v -> assertThat(
                v.getItem(), not(isEmptyString())));
        actualValues.stream().forEach(v -> assertThat(
                v.getAmount(), greaterThan(0.0)));
    }

    @Test
    public void maskCreditCardsAndFilterSmallPurchases() throws Exception {

        int expectedNumberOfRecords = 100;

        List<Purchase> previousValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        TRANSACTIONS_TOPIC,
                        expectedNumberOfRecords),
                Purchase.class);

        System.out.println(TRANSACTIONS_TOPIC + " received: " + previousValues);
        long filteredNumberOfRecords = previousValues.stream().filter(v -> v.getPrice() > 5.0).count();

        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TOPIC,
                        Math.toIntExact(filteredNumberOfRecords)),
                Purchase.class);

        System.out.println(PURCHASES_TOPIC + " received: " + actualValues);

        actualValues.stream().forEach(v -> assertThat(
                v.getCreditCardNumber(),
                matchesPattern("xxxx-xxxx-xxxx-\\d\\d\\d\\d")
        ));

        assertEquals(filteredNumberOfRecords, actualValues.stream().count());

        actualValues.stream().forEach(v -> assertThat(
                v.getPrice(),
                greaterThan(5.0)
        ));
    }

    @Test
    public void branchShoesAndFragances() throws Exception {

        int expectedNumberOfRecords = 100;

        List<Purchase> previousValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        TRANSACTIONS_TOPIC,
                        expectedNumberOfRecords),
                Purchase.class);

        System.out.println(TRANSACTIONS_TOPIC + " received: " + previousValues);

        long shoesNumberOfRecords = previousValues.stream()
                .filter(v -> v.getPrice() > 5.0)
                .filter(v -> v.getDepartment().equalsIgnoreCase("shoes"))
                .count();

        long fragrancesNumberOfRecords = previousValues.stream()
                .filter(v -> v.getPrice() > 5.0)
                .filter(v -> v.getDepartment().equalsIgnoreCase("fragrance"))
                .count();

        List<Purchase> shoesValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        SHOES_TOPIC,
                        Math.toIntExact(shoesNumberOfRecords)),
                Purchase.class);

        System.out.println(SHOES_TOPIC + " received: " + shoesValues);

        List<Purchase> fragrancesValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        FRAGRANCES_TOPIC,
                        Math.toIntExact(fragrancesNumberOfRecords)),
                Purchase.class);

        System.out.println(FRAGRANCES_TOPIC + " received: " + fragrancesValues);

        assertThat("number of shoes", shoesValues.stream().count(), greaterThan(0L));

        shoesValues.stream().forEach(v -> assertThat(
                v.getDepartment(),
                equalToIgnoringCase("shoes"))
        );

        assertThat("number of fragrances", fragrancesValues.stream().count(), greaterThan(0L));

        fragrancesValues.stream().forEach(v -> assertThat(
                v.getDepartment(),
                equalToIgnoringCase("fragrance"))
        );

    }


    @Test
    public void testRewardsAccumulator() throws Exception {

        int expectedNumberOfRecords = 100;
        List<RewardAccumulator> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        REWARDS_TOPIC,
                        expectedNumberOfRecords),
                RewardAccumulator.class);

        System.out.println(REWARDS_TOPIC + " received: " + actualValues);

        actualValues.stream().forEach(v -> assertThat(
                v.getCustomerId(),
                matchesPattern(".*,.*")));

        actualValues.stream().forEach(v -> assertThat(
                v.getPurchaseTotal(), greaterThan(0.0)
        ));

        actualValues.stream().forEach(v -> assertThat(
                v.getCurrentRewardPoints(), greaterThan(0)
        ));

        assertThat(actualValues.stream().filter(v -> v.getCurrentRewardPoints() < v.getTotalRewardPoints()).count(),
                greaterThan(1L));
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
