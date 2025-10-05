# Wikipedia Retention Project

### Cornell INFO 5306 / CS 5306 — Crowdsourcing Project

## 🧭 Goal
Understand how often **new Wikipedia editors return** after their first edit and what this reveals about early community engagement.

---

## 🔍 Motivation
Wikipedia thrives on volunteer contributions, yet many newcomers stop after a single edit.  
This project aims to **measure early retention**, providing insight into when and how editors drop off.

---

## 🔄 Pipeline Summary
1. Define **monthly cohorts** of brand-new editors (e.g., August–September 2024).
2. Fetch each user’s **first edit (t₀)** and **true second edit (t₁)** using the MediaWiki API.
3. Compute key retention metrics:
   - Edited again (any time after t₀)
   - Same-month retention
   - Next-month retention
   - Within 7 days / within 30 days
4. Generate clean CSV files and visual summaries for analysis.

---

## 📂 Repository Structure
```
wikipedia-retention/
├─ data/
│  ├─ raw/       # API outputs: first/second edits
│  ├─ clean/     # computed metrics and plots
├─ src/
│  ├─ collect/   # data collection scripts
│  │  ├─ rc_first_edits.py
│  │  ├─ first_edits.py
│  │  └─ merge_retention.py
│  └─ analysis/  # metric computation and plotting
│     ├─ analyze_retention.py
│     └─ summarize_and_plot.py
```
---

## 🧩 Key Files and Functions

### `rc_first_edits.py`
Scans monthly `recentchanges` windows to identify candidate editors.

### `merge_retention.py`
Determines each user’s **first edit (t₀)** and writes `rc_first_edits_YYYY-MM.csv`.

### `first_edits.py`
Fetches each user’s **true second edit (t₁)** strictly after t₀ and writes `rc_second_edits_YYYY-MM.csv`.

### `analyze_retention.py`
Joins t₀ and t₁ by `userid` to compute retention rates:
- `same_month`
- `next_month`
- `within_7d`
- `within_30d`

Outputs clean per-month CSVs (e.g., `retention_2024-08.csv`).

### `summarize_and_plot.py`
Aggregates monthly metrics, writes `retention_summary.csv`, and produces visualizations.

---

## ⚙️ How It Works
1. Collect editor data via MediaWiki API queries (`recentchanges`, `usercontribs`).
2. Create two datasets per month:
   - `rc_first_edits_YYYY-MM.csv` → earliest edit (t₀)
   - `rc_second_edits_YYYY-MM.csv` → next edit (t₁)
3. Join by `userid` and calculate how many users edited again.
4. Write metrics to `data/clean/retention_YYYY-MM.csv`.
5. Combine results into `retention_summary.csv` and plot a bar chart.

---

## 📊 Results
| Cohort Month | Users | Edited Again | Same Month | Next Month | Within 7d | Within 30d |
|---------------|--------|---------------|-------------|-------------|------------|-------------|
| 2024-08 | 105 | 47 (44.8%) | 47 (44.8%) | 0 (0%) | 47 (44.8%) | 47 (44.8%) |
| 2024-09 | 114 | 58 (50.9%) | 58 (50.9%) | 0 (0%) | 58 (50.9%) | 58 (50.9%) |

**Interpretation:** Nearly half of new editors made a second edit within a week and within the same month, but continued activity drops off quickly.

---

## 💬 Insights
- Early engagement is **high** (around 45–50% return rate in the same week).  
- However, **long-term retention is low** — very few edit beyond their first month.  
- This mirrors broader trends in online communities: quick curiosity, short commitment.

---

## 🧠 Next Steps
- Extend the window to include **subsequent months** to measure longer retention.  
- Add **topic-level or namespace segmentation** to see which types of pages keep users.  
- Compute **“3+ edits within 30 days”** as a stronger engagement signal.  
- Experiment with **survival analysis** or **Kaplan–Meier retention curves**.

---

## ⚙️ Reproduction Steps
```bash
# 0) Install dependencies
pip install python-dateutil matplotlib

# 1) Get true second edits (t₁)
python -u src/collect/first_edits.py

# 2) Get first edits (t₀)
python -u src/collect/merge_retention.py

# 3) Compute metrics
python -u src/analysis/analyze_retention.py

# 4) Summarize and plot
python -u src/analysis/summarize_and_plot.py
```

---

## 📁 Outputs
- `data/raw/rc_first_edits_YYYY-MM.csv` → first edits (t₀)
- `data/raw/rc_second_edits_YYYY-MM.csv` → true second edits (t₁)
- `data/clean/retention_YYYY-MM.csv` → monthly metrics
- `data/clean/retention_summary.csv` and `.png` → final summary chart

---

## 🧩 Team Reflection
This project demonstrates how data collection and analysis can uncover **behavioral dynamics in online communities**.  
By automating the retention pipeline, we created a reproducible method to measure engagement over time — a foundation for future social computing research.

---

## 🧾 Citation
If referencing, cite as:  
*Safdariyan, Andisha (2025). Wikipedia Retention Project — INFO/CS 5306 Crowdsourcing.*
