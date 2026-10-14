package edu.bigdata.clickstream.common;

import java.util.Objects;

/**
 * ClickEvent represents a parsed clickstream record.
 * Designed for safe reuse across map/reduce iterations.
 */
public class ClickEvent {

    private String eventTimeRaw = "";
    private long epochSeconds = 0L;
    private String logDate = "";
    private int hour = 0;
    private EventType eventType;
    private long productId = 0L;
    private long categoryId = 0L;
    private String categoryCode = "";
    private String brand = "";
    private double price = 0.0;
    private long userId = 0L;
    private String userSession = "";

    public ClickEvent() {
        // Default constructor
    }

    /**
     * Resets all fields to their defaults for object reuse in Hadoop mappers/reducers.
     */
    public void reset() {
        this.eventTimeRaw = "";
        this.epochSeconds = 0L;
        this.logDate = "";
        this.hour = 0;
        this.eventType = null;
        this.productId = 0L;
        this.categoryId = 0L;
        this.categoryCode = "";
        this.brand = "";
        this.price = 0.0;
        this.userId = 0L;
        this.userSession = "";
    }

    public String getEventTimeRaw() {
        return eventTimeRaw;
    }

    public void setEventTimeRaw(String eventTimeRaw) {
        this.eventTimeRaw = (eventTimeRaw == null) ? "" : eventTimeRaw;
    }

    public long getEpochSeconds() {
        return epochSeconds;
    }

    public void setEpochSeconds(long epochSeconds) {
        this.epochSeconds = epochSeconds;
    }

    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = (logDate == null) ? "" : logDate;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getCategoryCode() {
        return categoryCode;
    }

    public void setCategoryCode(String categoryCode) {
        this.categoryCode = (categoryCode == null) ? "" : categoryCode;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = (brand == null) ? "" : brand;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getUserSession() {
        return userSession;
    }

    public void setUserSession(String userSession) {
        this.userSession = (userSession == null) ? "" : userSession;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClickEvent that = (ClickEvent) o;
        return epochSeconds == that.epochSeconds &&
                hour == that.hour &&
                productId == that.productId &&
                categoryId == that.categoryId &&
                Double.compare(that.price, price) == 0 &&
                userId == that.userId &&
                Objects.equals(eventTimeRaw, that.eventTimeRaw) &&
                Objects.equals(logDate, that.logDate) &&
                eventType == that.eventType &&
                Objects.equals(categoryCode, that.categoryCode) &&
                Objects.equals(brand, that.brand) &&
                Objects.equals(userSession, that.userSession);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTimeRaw, epochSeconds, logDate, hour, eventType,
                productId, categoryId, categoryCode, brand, price, userId, userSession);
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "time='" + eventTimeRaw + '\'' +
                ", date='" + logDate + '\'' +
                ", hour=" + hour +
                ", type=" + eventType +
                ", product=" + productId +
                ", category=" + categoryId +
                ", catCode='" + categoryCode + '\'' +
                ", brand='" + brand + '\'' +
                ", price=" + price +
                ", user=" + userId +
                ", session='" + userSession + '\'' +
                '}';
    }
}
