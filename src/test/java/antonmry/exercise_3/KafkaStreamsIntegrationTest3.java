package antonmry.exercise_3;

import antonmry.clients.producer.MockDataProducer;
import antonmry.exercise_3.KafkaStreamsApp3;
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

public class KafkaStreamsIntegrationTest3 {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private static KafkaStreamsApp3 kafkaStreamsApp;
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

        kafkaStreamsApp = new KafkaStreamsApp3(properties);
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

}
