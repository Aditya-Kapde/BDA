package edu.bigdata.clickstream.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * ViewCountWritable encapsulates product and category view counts with associated
 * category and brand metadata for Job J3 (ViewCount) per SRS §3.4.1.
 */
public class ViewCountWritable implements Writable {

    private long count;
    private String category;
    private String brand;

    public ViewCountWritable() {
        this(0L, "", "");
    }

    public ViewCountWritable(long count, String category, String brand) {
        set(count, category, brand);
    }

    public void set(long count, String category, String brand) {
        this.count = count;
        this.category = (category == null) ? "" : category;
        this.brand = (brand == null) ? "" : brand;
    }

    public void reset() {
        set(0L, "", "");
    }

    /**
     * Merges another ViewCountWritable instance into this one.
     * Per SRS §3.3.6: Sum count; for category/brand keep the lexicographically
     * smallest non-empty string seen (deterministic).
     */
    public void merge(ViewCountWritable other) {
        if (other == null) {
            return;
        }
        this.count += other.count;

        // Deterministic category resolution: keep lexicographically smallest non-empty value
        if (this.category.isEmpty()) {
            this.category = other.category;
        } else if (!other.category.isEmpty() && other.category.compareTo(this.category) < 0) {
            this.category = other.category;
        }

        // Deterministic brand resolution: keep lexicographically smallest non-empty value
        if (this.brand.isEmpty()) {
            this.brand = other.brand;
        } else if (!other.brand.isEmpty() && other.brand.compareTo(this.brand) < 0) {
            this.brand = other.brand;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        WritableUtils.writeString(out, category == null ? "" : category);
        WritableUtils.writeString(out, brand == null ? "" : brand);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readLong();
        this.category = WritableUtils.readString(in);
        this.brand = WritableUtils.readString(in);
        if (this.category == null) this.category = "";
        if (this.brand == null) this.brand = "";
    }

    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }

    public String getCategory() { return category; }
    public void setCategory(String category) {
        this.category = (category == null) ? "" : category;
    }

    public String getBrand() { return brand; }
    public void setBrand(String brand) {
        this.brand = (brand == null) ? "" : brand;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ViewCountWritable that = (ViewCountWritable) o;
        return count == that.count &&
                Objects.equals(category, that.category) &&
                Objects.equals(brand, that.brand);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, category, brand);
    }

    @Override
    public String toString() {
        return count + "\t" + category + "\t" + brand;
    }
}
