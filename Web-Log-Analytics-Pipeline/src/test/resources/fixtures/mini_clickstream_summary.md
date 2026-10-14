# Mini Clickstream Test Fixture Specification
## File: src/test/resources/fixtures/mini_clickstream.csv

This document details the expected classification for each row in the test fixture according to SRS §3.3.3 and §3.4.2.

| Row # | Content Summary | Expected Status | Quality Counter / Note |
|---|---|---|---|
| 1 | `event_time,event_type,...` | `HEADER` | `HEADER_SKIPPED` |
| 2 | `2019-11-01 00:00:02 UTC,view,1001,...` | `VALID` | `VALID_RECORDS`, View count for 1001 |
| 3 | `2019-11-01 00:00:40 UTC,cart,1001,...` | `VALID` | `VALID_RECORDS`, Cart event for sess-A |
| 4 | `2019-11-01 00:59:59 UTC,view,1002,2002,,,5.00,...` | `VALID` | `VALID_RECORDS`, `MISSING_CATEGORY`, `MISSING_BRAND` |
| 5 | `2019-11-01 01:00:00 UTC,view,1001,...` | `VALID` | `VALID_RECORDS`, sess-C |
| 6 | `2019-11-01 01:10:00 UTC,purchase,1001,...` | `VALID` | `VALID_RECORDS`, Purchase event for sess-A |
| 7 | `2019-13-01 01:10:00 UTC,view,1001,...` | `MALFORMED` | `MALFORMED_TIMESTAMP` (Month 13 invalid) |
| 8 | `2019-11-01 01:20:00 UTC,view,abc,2001,...` | `MALFORMED` | `MALFORMED_ID` (Attributable to 2019-11-01 01) |
| 9 | `2019-11-01 01:30:00 UTC,view,1001,2001` | `MALFORMED` | `MALFORMED_COLUMN_COUNT` (4 columns < 9) |
| 10| *(Blank Line)* | `MALFORMED` | `MALFORMED_COLUMN_COUNT` |
| 11| `...extra_col` (10 columns) | `MALFORMED` | `MALFORMED_COLUMN_COUNT` (10 columns > 9) |
| 12| `...checkout,...` | `MALFORMED` | `MALFORMED_EVENT_TYPE` (`checkout` not in enum) |
| 13| `...price: -15.00...` | `MALFORMED` | `MALFORMED_PRICE` (Negative price) |
| 14| `...user_session: (empty)` | `MALFORMED` | `MALFORMED_SESSION` (Empty session string) |
| 15| `2019-11-01 23:59:59 UTC,cart,...` | `VALID` | `VALID_RECORDS`, sess-Z on 2019-11-01 |
| 16| `2019-11-02 00:00:01 UTC,remove_from_cart,...` | `VALID` | `VALID_RECORDS`, sess-Z first date = 2019-11-01 |
| 17| `2019-11-01 01:00:00 UTC,view,1001,...` | `VALID` | `VALID_RECORDS`, Duplicate of row 5 |
| 18| `2019-11-01 02:00:00 UTC,view,1004,...` | `VALID` | `VALID_RECORDS`, Top-N ranking candidate |
| 19| `2019-11-01 02:05:00 UTC,view,1005,...` | `VALID` | `VALID_RECORDS`, Top-N tie candidate |
