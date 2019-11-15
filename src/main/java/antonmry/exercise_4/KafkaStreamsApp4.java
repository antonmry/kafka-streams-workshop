package antonmry.exercise_4;

import antonmry.exercise_4.joiner.PurchaseJoiner;
import antonmry.exercise_4.partitioner.RewardsStreamPartitioner;
import antonmry.exercise_4.transformer.PurchaseRewardTransformer;
import antonmry.model.CorrelatedPurchase;
import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
import antonmry.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsApp4 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsApp4.class);

    public String getTopology() {
        return topology;
    }

    private final String topology;

    private KafkaStreams kafkaStreams;

    public KafkaStreamsApp4(Properties properties) {

        StreamsConfig streamsConfig = new StreamsConfig(properties);

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();

        Serde<CorrelatedPurchase> correlatedPurchaseSerde = new StreamsSerdes.CorrelatedPurchaseSerde();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Purchase> transactionKStream = streamsBuilder
                .stream("transactions", Consumed.with(stringSerde, purchaseSerde));

        KStream<String, Purchase> purchaseKStream = transactionKStream
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase
                .getPurchaseDate().getTime();

        KStream<Long, Purchase> filteredKStream = purchaseKStream
                .filter((key, purchase) -> purchase.getPrice() > 5.00)
                .selectKey(purchaseDateAsKey);

        filteredKStream.to("purchases", Produced.with(Serdes.Long(), purchaseSerde));

        String rewardsStateStoreName = "rewardsPointsStore";
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);

        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());

        streamsBuilder.addStateStore(storeBuilder);

        KStream<String, Purchase> transByCustomerStream = purchaseKStream
                .through("customer_transactions", Produced.with(stringSerde, purchaseSerde, streamPartitioner));

        KStream<String, RewardAccumulator> statefulRewardAccumulator = transByCustomerStream.
                transformValues(() -> new PurchaseRewardTransformer(rewardsStateStoreName),
                        rewardsStateStoreName);

        statefulRewardAccumulator.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        Predicate<String, Purchase> isShoe = (key, purchase) -> purchase.getDepartment()
                .equalsIgnoreCase("shoes");

        Predicate<String, Purchase> isFragrance = (key, purchase) -> purchase.getDepartment()
                .equalsIgnoreCase("fragrance");

        // TODO: complete `src/main/java/antonmry/exercise_4/joiner/PurchaseJoiner.java`

        // TODO: create the branch KStream using the customerId as key

        // TODO: create the shoes KStream and the fragrances KStream

        // TODO: ingest the previous KStreams in topics "shoes" and "fragrances"
        //  Note: this step isn't required

        // TODO: create a new instance of the PurchaseJoiner
        //  Note: https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.KStream-org.apache.kafka.streams.kstream.ValueJoiner-org.apache.kafka.streams.kstream.JoinWindows-

        // TODO: create a twenty minute window

        // TODO: create the new join stream using the previously created joiner and window

        // TODO: ingest the join KStream in the topic "shoesAndFragrancesAlerts"

        // TODO (Homework): How many state stores are created because of the join?

        // TODO (Homework): modify the window to keep the twenty minutes but having order so we make sure the
        //  shoes purchase occurs at least 5 minutes (or less) after the fragrance purchase

        // TODO (Homework): for the join window you are using the timestamp placed in the metadata when the event is
        //  added to the log but this isn't exactly the requirement. Adapt the code to use the purchaseDate inside the
        //  event.

        this.kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        this.topology = streamsBuilder.build().describe().toString();
    }

    void start() {

        LOG.info("Kafka Streams Application Started");
        kafkaStreams.start();
    }

    void stop() {
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
    }

    public static void main(String[] args) throws Exception {
        KafkaStreamsApp4 kafkaStreamsApp = new KafkaStreamsApp4(getProperties());
        kafkaStreamsApp.start();
        Thread.sleep(65000);
        kafkaStreamsApp.stop();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}
