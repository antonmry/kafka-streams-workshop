package antonmry.exercise_1.model;

import antonmry.model.Purchase;

public class RewardAccumulator {

    private String customerId;
    private double purchaseTotal;
    private int totalRewardPoints;
    private int currentRewardPoints;
    private int daysFromLastPurchase;

    private RewardAccumulator(String customerId, double purchaseTotal, int rewardPoints) {
        this.customerId = customerId;
        this.purchaseTotal = purchaseTotal;
        this.currentRewardPoints = rewardPoints;
        this.totalRewardPoints = rewardPoints;
    }

    public String getCustomerId() {
        return customerId;
    }

    public double getPurchaseTotal() {
        return purchaseTotal;
    }

    public int getCurrentRewardPoints() {
        return currentRewardPoints;
    }

    public int getTotalRewardPoints() {
        return totalRewardPoints;
    }

    public void addRewardPoints(int previousTotalPoints) {
        this.totalRewardPoints += previousTotalPoints;
    }

    @Override
    public String toString() {
        return "RewardAccumulator{" +
                "customerId='" + customerId + '\'' +
                ", purchaseTotal=" + purchaseTotal +
                ", totalRewardPoints=" + totalRewardPoints +
                ", currentRewardPoints=" + currentRewardPoints +
                ", daysFromLastPurchase=" + daysFromLastPurchase +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RewardAccumulator)) return false;

        RewardAccumulator that = (RewardAccumulator) o;

        if (Double.compare(that.purchaseTotal, purchaseTotal) != 0) return false;
        if (totalRewardPoints != that.totalRewardPoints) return false;
        if (currentRewardPoints != that.currentRewardPoints) return false;
        if (daysFromLastPurchase != that.daysFromLastPurchase) return false;
        return customerId != null ? customerId.equals(that.customerId) : that.customerId == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = customerId != null ? customerId.hashCode() : 0;
        temp = Double.doubleToLongBits(purchaseTotal);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + totalRewardPoints;
        result = 31 * result + currentRewardPoints;
        result = 31 * result + daysFromLastPurchase;
        return result;
    }

    public static Builder builder(Purchase purchase) {
        return new Builder(purchase);
    }

    public static final class Builder {
        private String customerId;
        private double purchaseTotal;
        private int daysFromLastPurchase;
        private int rewardPoints;

        private Builder(Purchase purchase) {

            // TODO: assign customerId using the first and the last name
            this.customerId = purchase.getLastName() + "," + purchase.getFirstName();

            // TODO: assign purchaseTotal based in the price and quantity
            this.purchaseTotal = purchase.getPrice() * (double) purchase.getQuantity();

            // TODO: the customer earns a point per euro, and transaction totals are rounded down to the nearest euro
            this.rewardPoints = (int) purchaseTotal;
        }


        public RewardAccumulator build() {
            return new RewardAccumulator(customerId, purchaseTotal, rewardPoints);
        }

    }
}
