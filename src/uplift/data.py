"""
Download the X5 RetailHero uplift dataset and convert it to Parquet.

These are the files scikit-uplift's ``fetch_x5`` uses. We fetch them directly rather 
than calling ``fetch_x5`` because it reads the whole purchase log (~45M rows) into
pandas at once, which does not fit in memory on a 16GB machine. DuckDB streams the 
gzipped CSV into Parquet instead, so peak memory stays bounded.

Run with ``uv run python -m uplift.data``.
"""


from __future__ import annotations

import argparse
import shutil
import urllib.request
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"


BASE_URL = "https://sklift.s3.eu-west-2.amazonaws.com"
TABLES = ("clients", "uplift_train", "purchases")


def raw_path(table : str) -> Path:
    
    """
    This function takes String as input (table name)  
    and returns a Path object that points to where that table's 
    compressed CSV file is located.
    """
    
    return RAW_DIR / f"{table}.csv.gz"

def parquet_path(table : str) -> Path:
    
    """
    This function takes String as input (table name)  
    and returns a Path object that points to where the table's
    .parquet file is located
    """
    
    return PARQUET_DIR / f"{table}.parquet"

def download(table : str, *, force: bool = False) -> Path:
    
    """
    This function takes string as input and fetches table's 
    gzipped csv , it skips if it already exists on disk
    """
    

    dest = raw_path(table)
    if dest.exists() and not force:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    with urllib.request.urlopen(f"{BASE_URL}/{table}.csv.gz") as resp, part.open("wb") as out:
        expected = int(resp.headers["Content-Length"])
        shutil.copyfileobj(resp, out, length=1 << 20)
    # A dropped connection leaves a short file; renaming only on a full read means a
    # truncated download is never mistaken for a finished one on the next run.
    got = part.stat().st_size
    if got != expected:
        part.unlink()
        raise OSError(f"{table}: downloaded {got:,} bytes, expected {expected:,}")
    part.replace(dest)
    return dest


def to_parquet(src: Path, dest: Path, *, memory_limit: str = "2GB") -> int:
    """Stream a gzipped CSV into a Parquet file and return its row count."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute(
            f"COPY (SELECT * FROM read_csv('{src}')) TO '{part}' "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{part}')").fetchone()[0]
    finally:
        con.close()
    part.replace(dest)
    return rows 

def main() -> None:
    parser = argparse.ArgumentParser(description="Download X5 RetailHero and convert to Parquet.")
    parser.add_argument("--force", action="store_true", help="re-download and re-convert")
    args = parser.parse_args()
    
    for table in TABLES:
        src = download(table, force = args.force)
        dest = parquet_path(table)
        if dest.exists() and not args.force:
            print(f"{table}: {dest.relative_to(PROJECT_ROOT)} exists, skipping")
            continue
        rows = to_parquet(src, dest)
        print(f"{table}: {rows:,} rows -> {dest.relative_to(PROJECT_ROOT)}")
        
        
if __name__ == "__main__":
    main()
        