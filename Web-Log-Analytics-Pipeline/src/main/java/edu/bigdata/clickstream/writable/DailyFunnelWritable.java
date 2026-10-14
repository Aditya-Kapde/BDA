package edu.bigdata.clickstream.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * DailyFunnelWritable encapsulates aggregated funnel metrics for a calendar date
 * per SRS §3.3.9 (FR-MR-15).
 *
 * Fields:
 * - sessions: total distinct valid sessions
 * - cart: total cart-participating sessions
 * - purchase: total purchase-completing sessions
 * - abandoned: total cart-abandoning sessions (cart without purchase)
 * - removes: total remove_from_cart events in cart sessions
 */
public class DailyFunnelWritable implements Writable {

    private long sessions;
    private long cart;
    private long purchase;
    private long abandoned;
    private long removes;

    public DailyFunnelWritable() {
        this(0L, 0L, 0L, 0L, 0L);
    }

    public DailyFunnelWritable(long sessions, long cart, long purchase, long abandoned, long removes) {
        this.sessions = sessions;
        this.cart = cart;
        this.purchase = purchase;
        this.abandoned = abandoned;
        this.removes = removes;
    }

    public void set(long sessions, long cart, long purchase, long abandoned, long removes) {
        this.sessions = sessions;
        this.cart = cart;
        this.purchase = purchase;
        this.abandoned = abandoned;
        this.removes = removes;
    }

    public void reset() {
        this.sessions = 0L;
        this.cart = 0L;
        this.purchase = 0L;
        this.abandoned = 0L;
        this.removes = 0L;
    }

    /**
     * Associative and commutative element-wise addition.
     */
    public void merge(DailyFunnelWritable other) {
        if (other == null) return;
        this.sessions += other.sessions;
        this.cart += other.cart;
        this.purchase += other.purchase;
        this.abandoned += other.abandoned;
        this.removes += other.removes;
    }

    public long getSessions() {
        return sessions;
    }

    public void setSessions(long sessions) {
        this.sessions = sessions;
    }

    public long getCart() {
        return cart;
    }

    public void setCart(long cart) {
        this.cart = cart;
    }

    public long getPurchase() {
        return purchase;
    }

    public void setPurchase(long purchase) {
        this.purchase = purchase;
    }

    public long getAbandoned() {
        return abandoned;
    }

    public void setAbandoned(long abandoned) {
        this.abandoned = abandoned;
    }

    public long getRemoves() {
        return removes;
    }

    public void setRemoves(long removes) {
        this.removes = removes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(sessions);
        out.writeLong(cart);
        out.writeLong(purchase);
        out.writeLong(abandoned);
        out.writeLong(removes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sessions = in.readLong();
        this.cart = in.readLong();
        this.purchase = in.readLong();
        this.abandoned = in.readLong();
        this.removes = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DailyFunnelWritable that = (DailyFunnelWritable) o;
        return sessions == that.sessions &&
                cart == that.cart &&
                purchase == that.purchase &&
                abandoned == that.abandoned &&
                removes == that.removes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessions, cart, purchase, abandoned, removes);
    }

    @Override
    public String toString() {
        return sessions + "\t" + cart + "\t" + purchase + "\t" + abandoned + "\t" + removes;
    }
}
