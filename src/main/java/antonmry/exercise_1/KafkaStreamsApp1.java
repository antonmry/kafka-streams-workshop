package antonmry.exercise_1;

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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsApp1 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsApp1.class);

    public String getTopology() {
        return topology;
    }

    private final String topology;

    private KafkaStreams kafkaStreams;

    public KafkaStreamsApp1(Properties properties) {

        StreamsConfig streamsConfig = new StreamsConfig(properties);

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        // TODO: create a purchasePattern Serde
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        // TODO: create a rewardAccumulator Serde
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Purchase> transactionKStream = streamsBuilder
                .stream("transactions", Consumed.with(stringSerde, purchaseSerde));

        KStream<String, Purchase> purchaseKStream = transactionKStream
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde));

        // TODO: create a pattern KStream
        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());

        // TODO: ingest in the topic "patterns" using the purchasePattern serde
        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        // TODO: create a rewardAccumulator KStream
        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.
                mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        // TODO: ingest in the topic "rewards" using the rewardAccumulator serde
        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // TODO (OPTIONAL): change from JSON to Avro serialization
        //  See https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro

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
        KafkaStreamsApp1 kafkaStreamsApp = new KafkaStreamsApp1(getProperties());
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
