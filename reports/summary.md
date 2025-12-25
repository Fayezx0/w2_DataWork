# Week 2 Summary — ETL + EDA

## Key findings
- **AE (United Arab Emirates) leads revenue**: AE generated approximately 325k in revenue with an Average Order Value (AOV) of ~258. Kuwait (KW), Qatar (QA), and Saudi Arabia (SA) follow closely.
- **Stable volumes across countries**: Order volumes are relatively consistent across all countries (n ~1.2k–1.3k), indicating that revenue differences are primarily driven by AOV rather than sheer order count.
- **Monthly Revenue Fluctuations**: Monthly revenue ranges from ~64k to ~118k. January showed strong performance (~106k), while December experienced a significant dip (~64k).
- **SA vs AE Comparison (Bootstrap)**: Statistical analysis (bootstrapping) of the difference in mean order amount between SA and AE shows a small difference (~ +0.23) with a 95% CI of [-11.08, +11.66], suggesting no decisive statistical difference in typical spending between these two markets.

## Definitions
- **Revenue**: Sum of `amount` (winsorized) for all orders.
- **Refund rate**: Total refund orders divided by total orders, where `status_clean` is "refund".
- **Average Order Value (AOV)**: Mean of `amount` (winsorized) per order.
- **Winsorized Amount**: Order amounts capped at the 1st and 99th percentiles to reduce the impact of extreme outliers.

## Data quality caveats
- **Missingness**: Approximately 507 orders (~9.6%) are missing `created_at` timestamps, which impacts the accuracy of time-based trends (recorded as `<NA>` in monthly reports).
- **Join coverage**: Join match rate for countries is 100% based on the available `user_id` links in the sampled data.
- **Outliers**: Order amounts were winsorized to handle extreme values. While this improves stability for general trends, it may hide significant high-value transactions relevant for financial audit.

## Next questions
- Why did December experience a significant revenue dip? Is this seasonal or related to specific data collection issues?
- What is the cause of the ~10% missingness in `created_at` timestamps, and can these be recovered from system logs?
- How do non-winsorized "whale" orders affect the overall revenue distribution?
