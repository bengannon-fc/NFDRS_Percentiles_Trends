# NFDRS Percentiles and Trends

**Purpose**

This code completes an automated pull of observed and forecasted fire danger data for Remote Automated Weather Stations (RAWS) from the Weather Information Management System (WIMS), and then translates Energy Release Component (ERC) and Burning Index (BI) raw values into percentiles, analyzes observed and forecasted trends for each index, and aggregates the same information up to the Predictive Service Area (PSA) level. The resulting spatial web service is intended to provide a high-level view of recently observed and forecasted fire danger for national and geographic area decision makers.

**Input data**
- Static copy of Remote Automated Weather Station (RAWS) locations from National Interagency Fire Center (https://data-nifc.opendata.arcgis.com/datasets/nifc::public-view-interagency-remote-automatic-weather-stations-raws/about), station identifiers and names quality controlled by geographic area lead.
- Static copy of Predictive Service Area (PSA) boundaries from the National Interagency Fire Center (https://data-nifc.opendata.arcgis.com/datasets/nifc::national-predictive-service-areas-psa-boundaries/about).
- Tables of key RAWS associated with each PSA and the historical percentiles of Energy Release Component (ERC) and Burning Index (BI). Exact data sources and methods vary by geographic area but generally include 20 years of observations for the full calendar year. All fire danger rating calculations in this service are standardized around fuel model Y (also known as “2016 forest”).
- Daily observations from the Weather Information Management System (WIMS) accessed at 16:30 Pacific: 1) most recent 3 days of daily observed weather, derived variables, ERC, and BI; and 2) next 3 days of daily forecasted ERC and BI.

**Analysis**
- The most recent day of observed and the next forecasted fire danger indices are converted to percentiles based on the historical percentile tables.
- Trend analysis categories determined by: 1) observed uses most recent daily observation compared to two days prior; 2) forecasted uses current day forecast compared to two days in the future; and 3) increase (>= +3), decrease (<= -3), or no change (< 3 diff) based on difference in absolute ERC or BI values, not percentiles.
- Aggregation to PSA: 1) non-reporting stations are ignored in calculations; 2) the PSA will be assigned a null value if it has no reporting stations, 3) simple means of RAWS percentiles; and 4) trends determined using simple means of index values from associated RAWS for equivalent time periods and same change thresholds (see above).
