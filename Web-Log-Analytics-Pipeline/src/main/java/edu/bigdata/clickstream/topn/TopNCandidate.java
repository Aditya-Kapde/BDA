package edu.bigdata.clickstream.topn;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TopNCandidate encapsulates a candidate record for Top-N ranking per SRS §3.3.7 (FR-MR-13).
 *
 * Ranking criteria (normative):
 *   1) views descending (higher views ranks first)
 *   2) key ascending (smaller key ranks first as deterministic tie-breaker)
 *      - numeric comparison if both keys are digits (e.g. product IDs)
 *      - lexicographical comparison for string keys (e.g. category codes)
 *   3) originalLine ascending if views and key are identical
 *   4) unique sequence ID tie-breaker so distinct candidate records are never collapsed
 *      or prematurely discarded by a bounded TreeMap / TreeSet.
 */
public class TopNCandidate implements Comparable<TopNCandidate> {

    private static final AtomicLong SEQ = new AtomicLong(0);

    private final long views;
    private final String key;
    private final String originalLine;
    private final long seqId;

    public TopNCandidate(long views, String key, String originalLine) {
        this.views = views;
        this.key = key != null ? key : "";
        this.originalLine = originalLine != null ? originalLine : "";
        this.seqId = SEQ.incrementAndGet();
    }

    // Constructor with explicit seqId for deterministic testing
    TopNCandidate(long views, String key, String originalLine, long seqId) {
        this.views = views;
        this.key = key != null ? key : "";
        this.originalLine = originalLine != null ? originalLine : "";
        this.seqId = seqId;
    }

    public long getViews() {
        return views;
    }

    public String getKey() {
        return key;
    }

    public String getOriginalLine() {
        return originalLine;
    }

    public long getSeqId() {
        return seqId;
    }

    /**
     * Parses a tab-delimited record from Job J3 output (products or categories).
     *
     * Expected schemas:
     *   - products:   product_id \t category_code \t brand \t views
     *   - categories: category_code \t views
     *
     * In both schemas:
     *   - key = parts[0]
     *   - views = parts[parts.length - 1] (non-negative integer/long)
     *
     * @param line raw text line from J3 output
     * @return TopNCandidate or null if line is blank, malformed, or has non-numeric count
     */
    public static TopNCandidate parse(String line) {
        if (line == null) {
            return null;
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
            return null;
        }

        String[] parts = trimmed.split("\t", -1);
        if (parts.length < 2) {
            return null;
        }

        String key = parts[0].trim();
        if (key.isEmpty()) {
            return null;
        }

        String viewsStr = parts[parts.length - 1].trim();
        try {
            long views = Long.parseLong(viewsStr);
            if (views < 0) {
                return null;
            }
            return new TopNCandidate(views, key, trimmed);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static int compareKeys(String k1, String k2) {
        if (k1 == null) k1 = "";
        if (k2 == null) k2 = "";
        if (!k1.isEmpty() && !k2.isEmpty() && isAllDigits(k1) && isAllDigits(k2)) {
            try {
                long n1 = Long.parseLong(k1);
                long n2 = Long.parseLong(k2);
                int cmp = Long.compare(n1, n2);
                if (cmp != 0) {
                    return cmp;
                }
            } catch (NumberFormatException ignored) {
                // fall back to string comparison if overflow occurs
            }
        }
        return k1.compareTo(k2);
    }

    private static boolean isAllDigits(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(TopNCandidate other) {
        if (this == other) {
            return 0;
        }
        // 1. Higher views rank first (descending)
        if (this.views != other.views) {
            return Long.compare(other.views, this.views);
        }
        // 2. Smaller key ranks first (ascending)
        int keyCmp = compareKeys(this.key, other.key);
        if (keyCmp != 0) {
            return keyCmp;
        }
        // 3. Tie-breaker for identical views and key: originalLine ascending
        int lineCmp = this.originalLine.compareTo(other.originalLine);
        if (lineCmp != 0) {
            return lineCmp;
        }
        // 4. Unique instance tie-breaker to prevent dropping distinct records
        return Long.compare(this.seqId, other.seqId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopNCandidate that = (TopNCandidate) o;
        return views == that.views &&
               seqId == that.seqId &&
               Objects.equals(key, that.key) &&
               Objects.equals(originalLine, that.originalLine);
    }

    @Override
    public int hashCode() {
        return Objects.hash(views, key, originalLine, seqId);
    }

    @Override
    public String toString() {
        return "TopNCandidate{views=" + views + ", key='" + key + "', seqId=" + seqId + "}";
    }
}
