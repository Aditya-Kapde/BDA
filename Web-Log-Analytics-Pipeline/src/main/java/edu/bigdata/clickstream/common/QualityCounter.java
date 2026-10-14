package edu.bigdata.clickstream.common;

/**
 * QualityCounter defines Hadoop MapReduce counters for data quality tracking
 * per SRS §3.3.2 (FR-MR-02) and Appendix C.
 *
 * Counter Group: ClickstreamQuality
 */
public enum QualityCounter {
    // Core Quality Counters (Group: ClickstreamQuality)
    TOTAL_RECORDS,
    VALID_RECORDS,
    HEADER_SKIPPED,
    MALFORMED_COLUMN_COUNT,
    MALFORMED_TIMESTAMP,
    MALFORMED_EVENT_TYPE,
    MALFORMED_ID,
    MALFORMED_PRICE,
    MALFORMED_SESSION,
    MISSING_CATEGORY,
    MISSING_BRAND,

    // Additional Pipeline Metric Counters (SRS Appendix C)
    VIEW_EVENTS,
    VIEWS_WITHOUT_CATEGORY,
    SESSIONS_SEEN,
    DATES_EMITTED;

    public static final String GROUP_NAME = "ClickstreamQuality";
}
