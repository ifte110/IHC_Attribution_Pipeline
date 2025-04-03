## Attribution Pipeline Report

### Overview
The Attribution Pipeline is designed to process customer journey data, calculate customer interaction phases (IHC), and generate channel performance reports. This report outlines the pipeline's functionality, key steps, and recent enhancements.

### Pipeline Steps
1. **Database Connection:**
   - Connects to the SQLite database (`challenge.db`).

2. **Data Fetching:**
   - Retrieves data from the following tables:
     - `session_sources` (session events)
     - `conversions` (conversion events)
     - `session_costs` (marketing costs)

3. **Customer Journey Construction:**
   - Builds customer journeys by linking sessions to conversions based on user IDs and timestamps.

4. **API Integration:**
   - Sends customer journeys to the IHC Attribution API in chunks of 100.
   - Receives IHC values and stores them in the `attribution_customer_journey` table.

5. **Channel Reporting Calculation:**
   - Aggregates data into the `channel_reporting` table, calculating:
     - **Cost:** Sum of session costs
     - **IHC:** Sum of IHC values
     - **IHC Revenue:** Sum of IHC multiplied by conversion revenue

6. **Filtering by Date Range (Optional):**
   - If a date range is provided, the pipeline processes only sessions and conversions within the range.
   - If no date is given, it defaults to processing all available data.

7. **Exporting Results:**
   - Exports channel performance data to `channel_reporting.csv`.
   - Calculates key metrics:
     - **CPO (Cost Per Order):** `cost / ihc`
     - **ROAS (Return on Ad Spend):** `ihc_revenue / cost`

### Enhancements
- **Date Range Filtering:**
  - Users can now specify a date range via `start_date` and `end_date`.
  - This allows for targeted analysis over specific time periods.
- **Fallback to Full Data:**
  - If no date range is provided, the pipeline defaults to processing all data without filtering.

### Conclusion
The pipeline efficiently processes customer journeys, calculates key attribution metrics, and produces actionable channel reports. The addition of date range filtering increases flexibility for time-specific analyses.

