package edu.bigdata.clickstream.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * EventType represents the four supported clickstream event types per SRS §3.1.1 and §3.3.3.
 * Supported values: view, cart, remove_from_cart, purchase.
 */
public enum EventType {
    VIEW("view", (byte) 0x01),
    CART("cart", (byte) 0x02),
    REMOVE_FROM_CART("remove_from_cart", (byte) 0x00),
    PURCHASE("purchase", (byte) 0x04);

    private final String csvValue;
    private final byte flagBit;

    private static final Map<String, EventType> LOOKUP;

    static {
        Map<String, EventType> map = new HashMap<>();
        for (EventType type : values()) {
            map.put(type.csvValue, type);
        }
        LOOKUP = Collections.unmodifiableMap(map);
    }

    EventType(String csvValue, byte flagBit) {
        this.csvValue = csvValue;
        this.flagBit = flagBit;
    }

    public String getCsvValue() {
        return csvValue;
    }

    public byte getFlagBit() {
        return flagBit;
    }

    /**
     * Resolves a CSV string value to an EventType.
     * Returns null if value is null, unknown, or malformed per SRS Rule 4.
     */
    public static EventType fromCsvValue(String value) {
        if (value == null) {
            return null;
        }
        return LOOKUP.get(value.trim());
    }

    @Override
    public String toString() {
        return csvValue;
    }
}
