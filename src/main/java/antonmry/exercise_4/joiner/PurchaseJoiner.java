package antonmry.exercise_4.joiner;

import antonmry.model.CorrelatedPurchase;
import antonmry.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {

    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {

        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();

        //TODO: create the CorrelatedPurchase combining purchase and otherPurchase


        // TODO: end

        return builder.build();
    }
}
