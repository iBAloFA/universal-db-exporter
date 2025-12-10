import typer, polars as pl
from sqlalchemy import create_engine
from tqdm import tqdm
import os

app = typer.Typer(help="Universal Database Exporter – any DB → any format")

SUPPORTED_FORMATS = ["csv", "json", "excel", "parquet"]

@app.command()
def export(
    db_url: str = typer.Argument(..., help="Database URL"),
    table: str = typer.Argument(..., help="Table name"),
    format: str = typer.Option("csv", "--format", "-f", help="Output format"),
    output: str = typer.Option(None, "--output", "-o", help="Output file (auto-generated if empty)"),
):
    """Export any table to any format"""
    if format not in SUPPORTED_FORMATS:
        typer.echo(f"Format must be one of: {', '.join(SUPPORTED_FORMATS)}")
        raise typer.Exit(1)

    engine = create_engine(db_url)
    
    # Get total rows for progress bar
    try:
        total = engine.execute(f"SELECT COUNT(*) FROM {table}").scalar()
    except:
        total = None

    typer.echo(f"Exporting {table} ({total:,} rows) → {format.upper()} ...")

    # Stream in chunks for huge tables
    chunks = []
    with engine.connect() as conn:
        for chunk_df in tqdm(pd.read_sql_table(table, engine, chunksize=50_000), total=total//50_000 + 1):
            # Convert to Polars for speed + Nigerian formatting
            pl_df = pl.from_pandas(chunk_df)
            
            # Fix common Nigerian formats
            for col in pl_df.columns:
                if "salary" in col.lower() or "amount" in col.lower() or "price" in col.lower():
                    pl_df = pl_df.with_columns(pl_df[col].cast(pl.Float64))
                elif "phone" in col.lower():
                    pl_df = pl_df = pl_df.with_columns(pl_df[col].cast(pl.Utf8))
                elif "date" in col.lower() or "time" in col.lower():
                    pl_df = pl_df.with_columns(pl.col(col).str.strptime(pl.Date, "%Y-%m-%d", strict=False))

            chunks.append(pl_df)

    final_df = pl.concat(chunks)

    # Generate filename
    if not output:
        output = f"{table}_export.{format if format != 'excel' else 'xlsx'}"

    # Write with correct extension
    if format == "csv":
        final_df.write_csv(output)
    elif format == "json":
        final_df.write_ndjson(output)
    elif format == "excel":
        final_df.write_excel(output)
    elif format == "parquet":
        final_df.write_parquet(output)

    size_mb = os.path.getsize(output) / (1024*1024)
    typer.echo(f"EXPORT COMPLETE! → {output} ({size_mb:.1f} MB)")

if __name__ == "__main__":
    app()
