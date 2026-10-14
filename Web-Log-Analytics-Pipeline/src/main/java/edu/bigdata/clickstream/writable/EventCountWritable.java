package edu.bigdata.clickstream.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 * EventCountWritable encapsulates aggregated event counts and malformed records
 * for Job J1 (HourlyTraffic) per SRS §3.4.1.
 */
public class EventCountWritable implements Writable {

    private long views;
    private long carts;
    private long removes;
    private long purchases;
    private long malformed;

    /**
     * Default public constructor required for Hadoop reflection/deserialization.
     */
    public EventCountWritable() {
        this(0L, 0L, 0L, 0L, 0L);
    }

    public EventCountWritable(long views, long carts, long removes, long purchases, long malformed) {
        set(views, carts, removes, purchases, malformed);
    }

    /**
     * Reusable setter to avoid per-record object allocation in Hadoop mappers/reducers.
     */
    public void set(long views, long carts, long removes, long purchases, long malformed) {
        this.views = views;
        this.carts = carts;
        this.removes = removes;
        this.purchases = purchases;
        this.malformed = malformed;
    }

    public void reset() {
        set(0L, 0L, 0L, 0L, 0L);
    }

    /**
     * Associative and commutative field-wise addition for combiners and reducers.
     */
    public void merge(EventCountWritable other) {
        if (other != null) {
            this.views += other.views;
            this.carts += other.carts;
            this.removes += other.removes;
            this.purchases += other.purchases;
            this.malformed += other.malformed;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(views);
        out.writeLong(carts);
        out.writeLong(removes);
        out.writeLong(purchases);
        out.writeLong(malformed);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.views = in.readLong();
        this.carts = in.readLong();
        this.removes = in.readLong();
        this.purchases = in.readLong();
        this.malformed = in.readLong();
    }

    public long getViews() { return views; }
    public void setViews(long views) { this.views = views; }

    public long getCarts() { return carts; }
    public void setCarts(long carts) { this.carts = carts; }

    public long getRemoves() { return removes; }
    public void setRemoves(long removes) { this.removes = removes; }

    public long getPurchases() { return purchases; }
    public void setPurchases(long purchases) { this.purchases = purchases; }

    public long getMalformed() { return malformed; }
    public void setMalformed(long malformed) { this.malformed = malformed; }

    /**
     * Returns total valid events: views + carts + removes + purchases per SRS §3.3.4.
     */
    public long getTotal() {
        return views + carts + removes + purchases;
    }

    /**
     * Calculates data-quality error rate: malformed / (total + malformed).
     * Returns 0.0 when denominator is zero.
     */
    public double getErrorRate() {
        long denominator = getTotal() + malformed;
        if (denominator == 0L) {
            return 0.0;
        }
        return (double) malformed / (double) denominator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventCountWritable that = (EventCountWritable) o;
        return views == that.views &&
                carts == that.carts &&
                removes == that.removes &&
                purchases == that.purchases &&
                malformed == that.malformed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(views, carts, removes, purchases, malformed);
    }

    @Override
    public String toString() {
        return views + "\t" + carts + "\t" + removes + "\t" + purchases + "\t" + malformed;
    }
}
