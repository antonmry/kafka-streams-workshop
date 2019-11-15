package antonmry.exercise_1;

import antonmry.model.Purchase;
import antonmry.exercise_1.model.PurchasePattern;
import antonmry.exercise_1.model.RewardAccumulator;
import antonmry.exercise_1.util.StreamsSerdes;
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

        // TODO: Complete `src/main/java/antonmry/exercise_1/model/PurchasePattern.java`
        // TODO: Complete `src/main/java/antonmry/exercise_1/model/RewardAccumulator.java`
        //  IMPORTANT: in the rest of the exercises will use the completed versions:
        //      `src/main/java/antonmry/model/PurchasePattern.java`
        //      `src/main/java/antonmry/model/RewardAccumulator.java`


        // TODO: create a purchasePattern Serde
        //  See https://kafka.apache.org/11/javadoc/org/apache/kafka/common/serialization/Serdes.html

        // TODO: create a rewardAccumulator Serde

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Purchase> transactionKStream = streamsBuilder
                .stream("transactions", Consumed.with(stringSerde, purchaseSerde));

        KStream<String, Purchase> purchaseKStream = transactionKStream
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde));

        // TODO: create a pattern KStream

        // TODO: ingest in the topic "patterns" using the purchasePattern serde

        // TODO: create a rewardAccumulator KStream

        // TODO: ingest in the topic "rewards" using the rewardAccumulator serde

        // TODO (Homework): change from JSON to Avro serialization
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
