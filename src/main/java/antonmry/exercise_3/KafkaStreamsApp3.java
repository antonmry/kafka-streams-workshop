package antonmry.exercise_3;

import antonmry.exercise_3.partitioner.RewardsStreamPartitioner;
import antonmry.exercise_3.transformer.PurchaseRewardTransformer;
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

public class KafkaStreamsApp3 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsApp3.class);

    public String getTopology() {
        return topology;
    }

    private final String topology;

    private KafkaStreams kafkaStreams;

    public KafkaStreamsApp3(Properties properties) {

        StreamsConfig streamsConfig = new StreamsConfig(properties);

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();

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

        Predicate<String, Purchase> isShoe = (key, purchase) -> purchase.getDepartment()
                .equalsIgnoreCase("shoes");

        Predicate<String, Purchase> isFragrance = (key, purchase) -> purchase.getDepartment()
                .equalsIgnoreCase("fragrance");

        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isShoe, isFragrance);

        kstreamByDept[0].to("shoes", Produced.with(stringSerde, purchaseSerde));

        kstreamByDept[1].to("fragrances", Produced.with(stringSerde, purchaseSerde));

        String rewardsStateStoreName = "rewardsPointsStore";
        // TODO: create a new RewardsStreamPartitioner
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();

        // TODO: create a in memory KeyValueStore with name rewardsPointsStore
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);

        // TODO: create a new KeyValue Store Builder
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());

        // TODO: add the store builder to the topoloy
        streamsBuilder.addStateStore(storeBuilder);

        // TODO: create a new kstream partitoned by CustomerId.
        // Hint: review purchaseKstream methods and use an intermediary topic
        KStream<String, Purchase> transByCustomerStream = purchaseKStream
                .through("customer_transactions", Produced.with(stringSerde, purchaseSerde, streamPartitioner));

        // TODO: create a stateful kstream aggregated with the total points.
        // Hint: review transformValues method
        KStream<String, RewardAccumulator> statefulRewardAccumulator = transByCustomerStream.transformValues(() -> new PurchaseRewardTransformer(rewardsStateStoreName),
                rewardsStateStoreName);

        // TODO: ingest the previous Stream in the "rewards" topic
        statefulRewardAccumulator.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // TODO (OPTIONAL): investigate the advantage of `transformValues` over `transform` and configure the state
        // store to have a change log stored in a topic.

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
        KafkaStreamsApp3 kafkaStreamsApp = new KafkaStreamsApp3(getProperties());
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
