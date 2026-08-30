# Dataset choice — and why billing is about bytes, not rows

## The trap in "100 records for minimum billing"

BigQuery does not bill for rows returned. It bills for **bytes scanned**, and
`LIMIT 100` does **not** reduce that. A `SELECT * ... LIMIT 100` against a
large unpartitioned table scans every byte of every column referenced and bills
for all of it — the LIMIT only truncates what comes back.

Three things actually reduce the bill, and the extractor uses all three:

1. **Select named columns, never `*`.** BigQuery is columnar; unreferenced
   columns are not scanned. This is the single biggest lever.
2. **Filter on the partitioning column** so partition pruning applies. A day
   predicate on a date-partitioned table scans one day, not the table.
3. **Cap with `maximum_bytes_billed`** on the job itself, so a query that would
   scan more than the cap is *rejected by BigQuery* rather than run and billed.
   This is the hard stop. LIMIT is not a hard stop; this is.

The daily free tier is 1 TiB of query processing per month. Configured as
below, one run scans single-digit MB, so a daily run lands around three orders
of magnitude inside the free tier.

## Chosen dataset

`bigquery-public-data.austin_bikeshare.bikeshare_trips`

Why this one:

- **Small and partition-friendly.** Filtering on `start_time` keeps a run's
  scan tiny.
- **It has genuinely sensitive-shaped fields.** `subscriber_type` and rider
  demographics are the kind of attribute a masking policy exists for, and the
  trip endpoints are location data — which is PII in every regime that matters.
  A dataset with nothing worth protecting would make the encryption and Ranger
  masking stages theatre.
- **It produces charts worth putting in a PDF** — trips over time, duration
  distribution, station rankings.

### Fields and their classification

| Field | Classification | Handling |
|---|---|---|
| `trip_id` | non-sensitive | clear |
| `bikeid` | **quasi-identifier** | encrypted, masked to last 4 |
| `subscriber_type` | **sensitive** | encrypted, masked by category |
| `start_station_id` / `_name` | **location PII** | encrypted, masked |
| `end_station_id` / `_name` | **location PII** | encrypted, masked |
| `start_time` | non-sensitive | clear, partition key |
| `duration_minutes` | non-sensitive | clear, drives the charts |

## Fallback

If this dataset is unavailable in your project's region,
`bigquery-public-data.chicago_taxi_trips.taxi_trips` has the same shape with
money columns, at the cost of being much larger — the `maximum_bytes_billed`
cap matters more there, not less.
