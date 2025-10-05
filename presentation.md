# 🧠 Wikipedia Editor Retention Analysis

## Overview
This project studies **Wikipedia’s new editors** — specifically, how many return after making their very first edit.  
Wikipedia relies heavily on volunteer contributors, yet most newcomers don’t continue editing after their initial contribution.  
Our goal was to **quantify new-editor retention** and understand early engagement patterns.

## Data Collection
We used the **MediaWiki API** to collect edit data from **August and September 2024**.  
For each newly registered user:
- We identified their **first edit**.
- We then searched for the **next edit** that occurred afterward.

The result is a linked dataset of users’ first and second edits.

## Analysis Pipeline
We built a **Python data pipeline** to:
1. Collect and clean edit data using API calls.  
2. Join the first-edit and second-edit datasets by username.  
3. Compute monthly retention metrics:
   - Whether a user edited again within the same month.  
   - Whether they made another edit within a week.  
   - Whether they continued into the next month.

## Key Findings
- About **half of newcomers make another edit within the same week**.  
- However, **very few continue editing in later weeks or months**.  
- This suggests that while initial engagement is promising, **long-term participation remains rare**.

## Visualization
The retention bar chart highlights the drop-off pattern:
> 📊 High short-term engagement → sharp decline in sustained activity.

## Next Steps
Future work could explore:
- **Topic-specific retention** — what kinds of pages keep users engaged?  
- **Temporal trends** — whether retention patterns vary across months or seasons.  
- **Interventions** — possible ways to improve editor onboarding and retention.

---

**Author:** Andisha Safdariyan  
**Date:** Fall 2024  
**Course:** INFO 5306 – Crowdsourcing and Human Computation  
**Language:** Python  
**Data Source:** [MediaWiki API](https://www.mediawiki.org/wiki/API:Main_page)
