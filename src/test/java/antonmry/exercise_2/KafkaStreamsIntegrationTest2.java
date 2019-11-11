package antonmry.exercise_2;

import antonmry.clients.producer.MockDataProducer;
import antonmry.exercise_1.KafkaStreamsApp1;
import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
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

public class KafkaStreamsIntegrationTest2 {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private static KafkaStreamsApp2 kafkaStreamsApp;
    private static Properties producerConfig;
    private static Properties consumerConfig;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String PURCHASES_TOPIC = "purchases";
    private static final String PATTERNS_TOPIC = "patterns";
    private static final String REWARDS_TOPIC = "rewards";
    private static final String SHOES_TOPIC = "shoes";
    private static final String FRAGRANCES_TOPIC = "fragrances";

    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void setUpAll() throws Exception {
        EMBEDDED_KAFKA.createTopic(TRANSACTIONS_TOPIC);
        EMBEDDED_KAFKA.createTopic(PURCHASES_TOPIC);
        EMBEDDED_KAFKA.createTopic(PATTERNS_TOPIC);
        EMBEDDED_KAFKA.createTopic(REWARDS_TOPIC);

        Properties properties = StreamsTestUtils.getStreamsConfig("integrationTest",
                EMBEDDED_KAFKA.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        kafkaStreamsApp = new KafkaStreamsApp2(properties);
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

}
