package edu.bigdata.clickstream.common;

import java.util.ArrayList;
import java.util.List;

/**
 * ClickEventParser implements the single source of truth for CSV parsing
 * and data quality evaluation per SRS §3.3.2 (FR-MR-01) and §3.3.3.
 *
 * Rules are evaluated in strict normative order; the first failing rule determines the counter:
 * 1. Line starts with "event_time," -> HEADER (HEADER_SKIPPED)
 * 2. Field count != 9 (RFC 4180 CSV split, keeping empties, blank lines) -> MALFORMED_COLUMN_COUNT
 * 3. Timestamp valid calendar date/time (yyyy-MM-dd HH:mm:ss) -> MALFORMED_TIMESTAMP
 * 4. Event type in {view, cart, remove_from_cart, purchase} -> MALFORMED_EVENT_TYPE
 * 5. product_id, category_id, user_id parse as long -> MALFORMED_ID
 * 6. price parses as decimal and >= 0 -> MALFORMED_PRICE
 * 7. user_session non-empty -> MALFORMED_SESSION
 *
 * Informational counters for valid records:
 * - category_code empty -> MISSING_CATEGORY
 * - brand empty -> MISSING_BRAND
 *
 * Attributability: If Rule 3 passes before a later rule fails, the record is attributable
 * to its date/hour bucket; otherwise it is unattributable (INVALID_TIME).
 */
public class ClickEventParser {

    public enum ParseStatus {
        VALID,
        HEADER,
        MALFORMED
    }

    public static class ParseResult {
        private ParseStatus status;
        private QualityCounter counter;
        private boolean attributable;
        private String dateHour = "INVALID_TIME";
        private String date = "NA";
        private int hour = -1;
        private boolean missingCategory = false;
        private boolean missingBrand = false;
        private ClickEvent event;

        public ParseStatus getStatus() { return status; }
        public QualityCounter getCounter() { return counter; }
        public boolean isAttributable() { return attributable; }
        public String getDateHour() { return dateHour; }
        public String getDate() { return date; }
        public int getHour() { return hour; }
        public boolean isMissingCategory() { return missingCategory; }
        public boolean isMissingBrand() { return missingBrand; }
        public ClickEvent getEvent() { return event; }

        public void reset() {
            this.status = null;
            this.counter = null;
            this.attributable = false;
            this.dateHour = "INVALID_TIME";
            this.date = "NA";
            this.hour = -1;
            this.missingCategory = false;
            this.missingBrand = false;
            this.event = null;
        }
    }

    private final StringBuilder buffer = new StringBuilder(256);
    private final List<String> fieldList = new ArrayList<>(12);
    private final ParseResult result = new ParseResult();

    public ClickEventParser() {
        // Default constructor
    }

    /**
     * Parses a raw line of text into a ClickEvent, returning the evaluation ParseResult.
     * Never throws exceptions on malformed records.
     *
     * @param line Raw input CSV line
     * @param targetEvent Reusable event holder (populated if record is VALID)
     * @return ParseResult containing status, counter, attributability, and event reference
     */
    public ParseResult parse(CharSequence line, ClickEvent targetEvent) {
        result.reset();

        if (line == null) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_COLUMN_COUNT;
            return result;
        }

        String raw = line.toString();

        // Rule 1: Header Check (Line starts with "event_time,")
        if (raw.startsWith("event_time,")) {
            result.status = ParseStatus.HEADER;
            result.counter = QualityCounter.HEADER_SKIPPED;
            return result;
        }

        // Blank line check
        if (raw.trim().isEmpty()) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_COLUMN_COUNT;
            return result;
        }

        // Rule 2: CSV Tokenization (RFC 4180 compliance, keeping empty fields)
        fieldList.clear();
        boolean parseOk = tokenizeCsv(raw, fieldList);

        if (!parseOk || fieldList.size() != 9) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_COLUMN_COUNT;
            return result;
        }

        String fTime = fieldList.get(0).trim();
        String fType = fieldList.get(1).trim();
        String fProductId = fieldList.get(2).trim();
        String fCategoryId = fieldList.get(3).trim();
        String fCategoryCode = fieldList.get(4).trim();
        String fBrand = fieldList.get(5).trim();
        String fPrice = fieldList.get(6).trim();
        String fUserId = fieldList.get(7).trim();
        String fUserSession = fieldList.get(8).trim();

        // Rule 3: Timestamp validation
        if (!TimeUtil.isValidTimestamp(fTime)) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_TIMESTAMP;
            result.attributable = false;
            result.dateHour = "INVALID_TIME";
            return result;
        }

        // Rule 3 passed: Record is attributable to its date/hour bucket from here on
        result.attributable = true;
        result.date = TimeUtil.extractDate(fTime);
        result.hour = TimeUtil.extractHour(fTime);
        result.dateHour = result.date + " " + String.format("%02d", result.hour);

        // Rule 4: Event Type validation
        EventType eventType = EventType.fromCsvValue(fType);
        if (eventType == null) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_EVENT_TYPE;
            return result;
        }

        // Rule 5: IDs parsing as long (product_id, category_id, user_id)
        long productId;
        long categoryId;
        long userId;
        try {
            productId = parsePositiveLong(fProductId);
            categoryId = parsePositiveLong(fCategoryId);
            userId = parsePositiveLong(fUserId);
        } catch (NumberFormatException e) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_ID;
            return result;
        }

        // Rule 6: Price parsing as decimal and >= 0
        double price;
        try {
            price = Double.parseDouble(fPrice);
            if (Double.isNaN(price) || price < 0.0) {
                result.status = ParseStatus.MALFORMED;
                result.counter = QualityCounter.MALFORMED_PRICE;
                return result;
            }
        } catch (NumberFormatException e) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_PRICE;
            return result;
        }

        // Rule 7: User Session non-empty
        if (fUserSession.isEmpty()) {
            result.status = ParseStatus.MALFORMED;
            result.counter = QualityCounter.MALFORMED_SESSION;
            return result;
        }

        // All 7 rules passed -> Record is VALID
        result.status = ParseStatus.VALID;
        result.counter = QualityCounter.VALID_RECORDS;

        // Informational missing value checks
        if (fCategoryCode.isEmpty()) {
            result.missingCategory = true;
        }
        if (fBrand.isEmpty()) {
            result.missingBrand = true;
        }

        // Populate target ClickEvent if provided
        if (targetEvent != null) {
            targetEvent.reset();
            targetEvent.setEventTimeRaw(fTime);
            targetEvent.setEpochSeconds(TimeUtil.toEpochSeconds(fTime));
            targetEvent.setLogDate(result.date);
            targetEvent.setHour(result.hour);
            targetEvent.setEventType(eventType);
            targetEvent.setProductId(productId);
            targetEvent.setCategoryId(categoryId);
            targetEvent.setCategoryCode(fCategoryCode);
            targetEvent.setBrand(fBrand);
            targetEvent.setPrice(price);
            targetEvent.setUserId(userId);
            targetEvent.setUserSession(fUserSession);
            result.event = targetEvent;
        }

        return result;
    }

    /**
     * Parses positive long string digits. Rejects empty, negative, or non-digit chars.
     */
    private static long parsePositiveLong(String s) {
        if (s == null || s.isEmpty()) {
            throw new NumberFormatException("Empty ID string");
        }
        long val = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw new NumberFormatException("Invalid digit: " + c);
            }
            val = val * 10 + (c - '0');
        }
        return val;
    }

    /**
     * Fast RFC 4180 compliant CSV tokenizer.
     * Supports quoted fields, escaped quotes (""), and embedded commas.
     * Returns false if quotes are unclosed.
     */
    private boolean tokenizeCsv(String line, List<String> fields) {
        buffer.setLength(0);
        boolean inQuotes = false;
        int len = line.length();

        for (int i = 0; i < len; i++) {
            char c = line.charAt(i);

            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < len && line.charAt(i + 1) == '"') {
                        buffer.append('"');
                        i++; // Skip escaped quote
                    } else {
                        inQuotes = false;
                    }
                } else {
                    buffer.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(buffer.toString());
                    buffer.setLength(0);
                } else if (c != '\r' && c != '\n') {
                    buffer.append(c);
                }
            }
        }

        if (inQuotes) {
            return false; // Unclosed quote -> structural CSV error
        }

        fields.add(buffer.toString());
        return true;
    }
}
