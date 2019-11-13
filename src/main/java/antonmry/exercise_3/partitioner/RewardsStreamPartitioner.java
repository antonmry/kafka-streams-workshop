package antonmry.exercise_3.partitioner;

import antonmry.model.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;


public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {

    @Override
    public Integer partition(String key, Purchase value, int numPartitions) {
        // TODO: change null to partition equitably by the customerId
        return null;
    }
}
