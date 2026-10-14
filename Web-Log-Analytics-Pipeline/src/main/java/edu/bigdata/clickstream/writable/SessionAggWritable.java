package edu.bigdata.clickstream.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 * SessionAggWritable tracks session-level behavioral funnel state for Job J5
 * per SRS §3.3.8 and §3.4.1.
 *
 * Bitmask definitions for flagsMask:
 *   Bit 0 (0x01): VIEW
 *   Bit 1 (0x02): CART
 *   Bit 2 (0x04): PURCHASE
 */
public class SessionAggWritable implements Writable {

    public static final byte FLAG_VIEW = (byte) 0x01;
    public static final byte FLAG_CART = (byte) 0x02;
    public static final byte FLAG_PURCHASE = (byte) 0x04;

    private long minEpoch;
    private byte flagsMask;
    private int removeCount;

    public SessionAggWritable() {
        this(0L, (byte) 0, 0);
    }

    public SessionAggWritable(long minEpoch, byte flagsMask, int removeCount) {
        set(minEpoch, flagsMask, removeCount);
    }

    public void set(long minEpoch, byte flagsMask, int removeCount) {
        this.minEpoch = minEpoch;
        this.flagsMask = flagsMask;
        this.removeCount = removeCount;
    }

    public void reset() {
        set(0L, (byte) 0, 0);
    }

    /**
     * Associative and commutative combiner/reducer merge logic:
     * - minEpoch: minimum non-zero timestamp
     * - flagsMask: bitwise OR
     * - removeCount: field-wise sum
     */
    public void merge(SessionAggWritable other) {
        if (other == null) {
            return;
        }

        if (this.minEpoch == 0L) {
            this.minEpoch = other.minEpoch;
        } else if (other.minEpoch != 0L && other.minEpoch < this.minEpoch) {
            this.minEpoch = other.minEpoch;
        }

        this.flagsMask = (byte) (this.flagsMask | other.flagsMask);
        this.removeCount += other.removeCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(minEpoch);
        out.writeByte(flagsMask);
        out.writeInt(removeCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.minEpoch = in.readLong();
        this.flagsMask = in.readByte();
        this.removeCount = in.readInt();
    }

    public long getMinEpoch() { return minEpoch; }
    public void setMinEpoch(long minEpoch) { this.minEpoch = minEpoch; }

    public byte getFlagsMask() { return flagsMask; }
    public void setFlagsMask(byte flagsMask) { this.flagsMask = flagsMask; }

    public int getRemoveCount() { return removeCount; }
    public void setRemoveCount(int removeCount) { this.removeCount = removeCount; }

    public boolean hasView() {
        return (flagsMask & FLAG_VIEW) != 0;
    }

    public boolean hasCart() {
        return (flagsMask & FLAG_CART) != 0;
    }

    public boolean hasPurchase() {
        return (flagsMask & FLAG_PURCHASE) != 0;
    }

    /**
     * Determines whether session was abandoned per SRS §1.3 and §3.3.8:
     * Session with >= 1 cart event and 0 purchase events.
     */
    public boolean isAbandoned() {
        return hasCart() && !hasPurchase();
    }

    public void addFlag(byte flag) {
        this.flagsMask = (byte) (this.flagsMask | flag);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionAggWritable that = (SessionAggWritable) o;
        return minEpoch == that.minEpoch &&
                flagsMask == that.flagsMask &&
                removeCount == that.removeCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minEpoch, flagsMask, removeCount);
    }

    @Override
    public String toString() {
        return minEpoch + "\t" + flagsMask + "\t" + removeCount;
    }
}
