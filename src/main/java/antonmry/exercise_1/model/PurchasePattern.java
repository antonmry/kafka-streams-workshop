package antonmry.exercise_1.model;

import antonmry.model.Purchase;

import java.util.Date;

public class PurchasePattern {

    private String zipCode;
    private String item;
    private Date date;
    private double amount;


    private PurchasePattern(Builder builder) {
        zipCode = builder.zipCode;
        item = builder.item;
        date = builder.date;
        amount = builder.amount;

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder builder(Purchase purchase){
        return new Builder(purchase);

    }
    public String getZipCode() {
        return zipCode;
    }

    public String getItem() {
        return item;
    }

    public Date getDate() {
        return date;
    }

    public double getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "PurchasePattern{" +
                "zipCode='" + zipCode + '\'' +
                ", item='" + item + '\'' +
                ", date=" + date +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PurchasePattern)) return false;

        PurchasePattern that = (PurchasePattern) o;

        if (Double.compare(that.amount, amount) != 0) return false;
        if (zipCode != null ? !zipCode.equals(that.zipCode) : that.zipCode != null) return false;
        return item != null ? item.equals(that.item) : that.item == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = zipCode != null ? zipCode.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        temp = Double.doubleToLongBits(amount);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static final class Builder {
        private String zipCode;
        private String item;
        private Date date;
        private double amount;

        private  Builder() {
        }

        private Builder(Purchase purchase) {
            // TODO: obtain Zip Code from purchase
            this.zipCode = purchase.getZipCode();

            // TODO: obtain Item from purchase
            this.item = purchase.getItemPurchased();

            // TODO: obtain PurchaseDate from purchase
            this.date = purchase.getPurchaseDate();

            // TODO: amount should be the price multiply by the number of products
            this.amount = purchase.getPrice() * purchase.getQuantity();
        }

        public Builder zipCode(String val) {
            zipCode = val;
            return this;
        }

        public Builder item(String val) {
            item = val;
            return this;
        }

        public Builder date(Date val) {
            date = val;
            return this;
        }

        public Builder amount(double amount) {
            this.amount = amount;
            return this;
        }

        public PurchasePattern build() {
            return new PurchasePattern(this);
        }
    }
}
