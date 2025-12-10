# Universal DB Exporter

Export any database to any format in one line:

```bash
python export.py "postgresql://user:pass@localhost/mydb" employees --format excel

Supported sources: PostgreSQL · MySQL · SQLite · SQL Server · Oracle
Supported formats: csv · json · excel · parquet

Features

Auto-detects & preserves ₦ currency, Nigerian phone numbers, dates
Zero config – just give connection string
Blazing fast with Polars (10× faster than pandas)
Works offline, on Render, Railway, GitHub Actions
