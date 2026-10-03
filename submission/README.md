# Spark Capstone: Fix the Slow Pipeline

PySpark job that cleans messy e-commerce data, answers six business questions, and shows five before/after speed-ups with the Spark UI.

Code is also on GitHub: **<ADD GITHUB LINK HERE>**

## What you need

- Python 3 with `pyspark` (built and tested with PySpark 4.2.0)
- Java 17 or newer (Spark needs it)

```bash
pip install pyspark
```

## How to run

1. Generate the data (once, from the project root). It creates `data/orders.csv`, `data/customers.csv` and `data/products.csv`.

```bash
python data/make_data.py
```

2. Run the pipeline from inside `submission/`, because the script uses relative paths (`../data` and `out/`).

```bash
cd submission
python capstone.py
```

The full run takes a couple of minutes on a laptop. When it finishes in a terminal it pauses so the Spark UI stays open (the URL is printed). Take your screenshots, then press Enter to exit.

## What the script does

| Part | What happens |
|---|---|
| A | Cleans the data and prints a data quality report. Orphan orders are saved, not deleted. |
| B | Writes six answer CSVs to `out/` (question 1 is done in both the DataFrame API and SQL). |
| C | Five before/after proofs: broadcast join, shuffle partitions, skew and AQE, caching, UDF vs `when/otherwise`. It prints task counts and shuffle bytes from the Spark UI. |

## Output

```
submission/
    capstone.py
    out/            six Part B answers + orphan_orders/
    screenshots/    c1_before.png ... c5_after.png
    README.md
```

## Part A accounting

| Item | Count |
|---|---|
| Orders at start | 1,020,025 |
| Duplicates removed | 20,025 |
| Bad amounts removed | 15,022 |
| Bad timestamps removed | 4,859 |
| Orphans set aside | 9,892 |
| Clean orders remaining | 970,227 |
| Customers at start / duplicates removed / clean | 10,300 / 300 / 10,000 |
