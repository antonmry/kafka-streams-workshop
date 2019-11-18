package antonmry.exercise_2;

import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
import antonmry.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsApp2 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsApp2.class);

    public Topology getTopology() {
        return topology;
    }

    private final Topology topology;

    private KafkaStreams kafkaStreams;

    public KafkaStreamsApp2(Properties properties) {

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

        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.
                mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // TODO: create a new KeyValueMapper using the purchase date as key
        //  See https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/kstream/KeyValueMapper.html

        // TODO: create a new Kstream filtering by Price (bigger than 5.00) and
        //  adding the key created in the previous step

        // TODO: change the following line to ingest the previous KStream in the topic "purchases"
        purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde));


        // TODO: create a Predicate to identify purchases in the shoes department

        // TODO: create a Predicate to identify purchases in the fragance department

        // TODO: create an array of KStream with each branch (shoes and fragances)
        //  See https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/kstream/KStream.html#branch-org.apache.kafka.streams.kstream.Predicate...-

        // TODO: ingest the first KStream in the topic "shoes"

        // TODO: ingest the second KStream in the topic "fragrances"

        // TODO (Homework): launch the application and try to ingest and read from the topics using kafkacat
        // https://github.com/edenhill/kafkacat

        this.topology = streamsBuilder.build();
        this.kafkaStreams = new KafkaStreams(topology, properties);
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
        KafkaStreamsApp2 kafkaStreamsApp = new KafkaStreamsApp2(getProperties());
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
