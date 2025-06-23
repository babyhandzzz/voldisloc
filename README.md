# Volatility Dislocation Analytics Dashboard

## Project Summary

This project builds a dashboard for tracking and visualizing volatility dislocations in single-stock options, drawing on the analytical approaches described in the Barclays US Equity Derivatives Strategy report (Sep 2020)[1]. The dashboard is intended as a research and educational tool, not for trading or investment advice, and does **not** attempt to replicate or distribute Barclays' proprietary strategies or content.

---

## Key Analytical Insights (Meta Information from Barclays)

- **Retail Surge:** Retail investors have driven a sharp increase in short-dated call option trading, especially in large-cap tech stocks.
- **Volume vs. Open Interest:** Much of the new option volume is speculative, short-term trading (volume has grown much faster than open interest).
- **Short-Dated Focus:** Most new activity is in options expiring in less than two weeks.
- **Skew & Term Structure:** Increased call demand has flattened skew (call vs. put IV) and compressed the term structure (short-term IV vs. longer-term IV).
- **Volatility Risk Premium (VRP):** Implied volatility has risen, but so has realized volatility, so VRP hasn’t increased as much as expected.
- **Market Impact:** Option activity—especially delta-hedging by market makers—now has a measurable effect on underlying stock prices.
- **Normalized Metrics:** Metrics like option volume/open interest and delta-adjusted option volume help compare activity across stocks and time.

---

## Technical Stack

- **Python** for data processing and analytics
- **Google Cloud Platform (GCP)**
  - **BigQuery** for storing and querying options and stock data
  - **Secret Manager** for secure API key storage
- **Local Development** for now, with plans to migrate data pipelines and analytics to GCP services

---

## Data & Workflow

1. **Ingest Data:** Pull historical and live options data using APIs (e.g., Alpha Vantage), with credentials managed via Secret Manager.
2. **Store Data:** Upload raw and processed data to BigQuery for scalable analysis.
3. **Analyze:** Calculate key metrics:
    - Option volume/open interest
    - Skew and term structure
    - VRP (implied minus realized volatility)
    - Delta-adjusted volume
    - Normalized/excess option volume
4. **Visualize:** (Planned) Build dashboards to monitor and explore volatility dislocations across stocks and sectors.

---

## Legal and Ethical Notice

- **This project does not copy, redistribute, or republish the Barclays report or its proprietary content.**
- **No Barclays charts, tables, or text are included.** Only meta-level analytical ideas are used as inspiration.
- **All code, analytics, and visualizations are original work.**
- **Barclays is cited as the source of analytical inspiration only.**
- This use is consistent with fair use/fair dealing for research and educational purposes, as no substantial or proprietary content from the report is reproduced or distributed[1].

---

**Reference:**  
[1] Barclays US Equity Derivatives Strategy, Sep 2020 ([see disclosure and copyright statement in the original PDF](https://amarketplaceofideas.com/wp-content/uploads/2021/08/Barclays_US_Equity_Derivatives_Strategy_Impact_of_Retail_Options_Trading.pdf))

