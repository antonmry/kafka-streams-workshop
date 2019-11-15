package antonmry.solution;

import antonmry.clients.producer.MockDataProducer;
import antonmry.model.CorrelatedPurchase;
import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
import com.salesforce.kafka.test.KafkaTestCluster;
import com.salesforce.kafka.test.KafkaTestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class KafkaStreamsIntegrationTest {

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

    private static final KafkaTestCluster EMBEDDED_KAFKA = new KafkaTestCluster(1);

    @BeforeClass
    public static void setUpAll() throws Exception {

        EMBEDDED_KAFKA.start();

        final KafkaTestUtils utils = new KafkaTestUtils(EMBEDDED_KAFKA);

        utils.createTopic(TRANSACTIONS_TOPIC, 1, (short) 1);
        utils.createTopic(PURCHASES_TOPIC, 1, (short) 1);
        utils.createTopic(PATTERNS_TOPIC, 1, (short) 1);
        utils.createTopic(PURCHASES_TABLE_TOPIC, 1, (short) 1);
        utils.createTopic(REWARDS_TOPIC, 1, (short) 1);
        utils.createTopic(SHOES_TOPIC, 1, (short) 1);
        utils.createTopic(FRAGRANCES_TOPIC, 1, (short) 1);
        utils.createTopic(SHOES_AND_FRAGANCES_TOPIC, 1, (short) 1);

        Properties properties = StreamsTestUtils.getStreamsConfig("integrationTest",
                EMBEDDED_KAFKA.getKafkaConnectString(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        kafkaStreamsApp = new KafkaStreamsApp(properties);
        kafkaStreamsApp.start();

        producerConfig = TestUtils.producerConfig(EMBEDDED_KAFKA.getKafkaConnectString(),
                StringSerializer.class,
                StringSerializer.class);

        consumerConfig = TestUtils.consumerConfig(EMBEDDED_KAFKA.getKafkaConnectString(), "test",
                StringDeserializer.class,
                StringDeserializer.class);

        MockDataProducer.producePurchaseData(producerConfig);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        kafkaStreamsApp.stop();
        EMBEDDED_KAFKA.stop();
    }

    /**
     * Exercise 0
     */

    @Test
    public void maskCreditCards() throws Exception {

        MockDataProducer.producePurchaseData(producerConfig);

        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TOPIC,
                        100,
                        60000),
                Purchase.class);

        System.out.println("Received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getCreditCardNumber(),
                matchesPattern("xxxx-xxxx-xxxx-\\d\\d\\d\\d")
        ));

    }

    /**
     * Exercise 1
     */

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

        actualValues.forEach(v -> assertThat(
                v.getZipCode(), not(isEmptyString())));
        actualValues.forEach(v -> assertThat(
                v.getItem(), not(isEmptyString())));
        actualValues.forEach(v -> assertThat(
                v.getAmount(), greaterThan(0.0)));
    }

    /**
     * Exercise 2
     */

    @Test
    public void maskCreditCardsAndFilterSmallPurchases() throws Exception {

        List<Purchase> previousValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        TRANSACTIONS_TOPIC,
                        100,
                        60000),
                Purchase.class);

        System.out.println(TRANSACTIONS_TOPIC + " received: " + previousValues);

        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TOPIC,
                        100),
                Purchase.class);

        System.out.println(PURCHASES_TOPIC + " received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getCreditCardNumber(),
                matchesPattern("xxxx-xxxx-xxxx-\\d\\d\\d\\d")
        ));

        actualValues.forEach(v -> assertThat(
                v.getPrice(),
                greaterThan(5.0)
        ));
    }

    @Test
    public void branchShoesAndFragrances() throws Exception {

        List<Purchase> previousValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        TRANSACTIONS_TOPIC,
                        100,
                        60000),
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

        assertThat("number of shoes", (long) shoesValues.size(), greaterThan(0L));

        shoesValues.forEach(v -> assertThat(
                v.getDepartment(),
                equalToIgnoringCase("shoes"))
        );

        assertThat("number of fragrances", (long) fragrancesValues.size(), greaterThan(0L));

        fragrancesValues.forEach(v -> assertThat(
                v.getDepartment(),
                equalToIgnoringCase("fragrance"))
        );

    }

    /**
     * Exercise 3
     */

    @Test
    public void testRewardsAccumulator() throws Exception {

        List<RewardAccumulator> actualValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        REWARDS_TOPIC,
                        100,
                        60000),
                RewardAccumulator.class);

        System.out.println(REWARDS_TOPIC + " received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getCustomerId(),
                matchesPattern(".*,.*")));

        actualValues.forEach(v -> assertThat(
                v.getPurchaseTotal(), greaterThan(0.0)
        ));

        actualValues.forEach(v -> assertThat(
                v.getCurrentRewardPoints(), greaterThan(0)
        ));

        assertThat(actualValues.stream().filter(v -> v.getCurrentRewardPoints() < v.getTotalRewardPoints()).count(),
                greaterThan(1L));
    }

    /**
     * Exercise 4
     */

    @Test
    public void joinShoesAndFragances() throws Exception {

        List<CorrelatedPurchase> previousValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        SHOES_AND_FRAGANCES_TOPIC,
                        20,
                        60000),
                CorrelatedPurchase.class);

        System.out.println(SHOES_AND_FRAGANCES_TOPIC + " received: " + previousValues);
        System.out.println(SHOES_AND_FRAGANCES_TOPIC + " count: " + (long) previousValues.size());

        assertThat((long) previousValues.size(), greaterThan(1L));

        previousValues.forEach(v -> assertThat(
                v.getCustomerId(),
                notNullValue())
        );

        previousValues.forEach(v -> assertThat(
                v.getFirstPurchaseTime(),
                notNullValue())
        );

        previousValues.forEach(v -> assertThat(
                v.getSecondPurchaseTime(),
                notNullValue())
        );

        previousValues.forEach(v -> assertThat(
                (long) v.getItemsPurchased().size(),
                greaterThan(1L))
        );
    }

    /**
     * Exercise 5
     */

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
        System.out.println(PURCHASES_TOPIC + " count: " + (long) actualValues.size());

        List<Purchase> tableValues = MockDataProducer.convertFromJson(
                IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                        consumerConfig,
                        PURCHASES_TABLE_TOPIC,
                        20,
                        60000),
                Purchase.class);

        System.out.println(PURCHASES_TABLE_TOPIC + " received: " + tableValues);
        System.out.println(PURCHASES_TABLE_TOPIC + " count: " + (long) tableValues.size());

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

/*
    @Test
    public void printTopology() throws Exception {
        System.out.println(kafkaStreamsApp.getTopology());
    }
*/
}
