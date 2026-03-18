# San José Budget & Compensation Intelligence Dashboard

Interactive analysis of City of San José employee compensation spending — the largest line item in the city's operating budget.

## What It Analyzes

- **Total spending** by year, department, and compensation category
- **Overtime waste** — departments where OT exceeds 30% of base pay
- **Extreme OT cases** — employees earning more in overtime than base salary
- **Year-over-year trends** — spending growth, headcount changes, cost-per-employee
- **Top earners** — highest paid employees and compensation distribution

## Data Source

All data pulled live from [data.sanjoseca.gov](https://data.sanjoseca.gov/dataset/employee-compensation-plan) — Employee Compensation Plans (2018–2024), published by the Finance Department under CC0 license.

## Deploy to Streamlit Community Cloud

```bash
git init && git add . && git commit -m "init"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/sj-budget-dashboard.git
git push -u origin main
```

Then go to [share.streamlit.io](https://share.streamlit.io), connect your repo, set `app.py` as main file, and deploy.

## Local Development

```bash
pip install -r requirements.txt
streamlit run app.py
```
# sj-budget-dashboard
