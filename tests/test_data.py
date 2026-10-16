import gzip

import duckdb

from uplift.data import to_parquet


def test_to_parquet_round_trips_a_gzipped_csv(tmp_path):
    src = tmp_path / "experiment.csv.gz"
    with gzip.open(src, "wt") as f:
        f.write("client_id,treatment_flg,target\na1,1,0\nb2,0,1\nc3,1,1\n")
    dest = tmp_path / "experiment.parquet"

    rows = to_parquet(src, dest)

    assert rows == 3
    assert not dest.with_name(dest.name + ".part").exists()
    got = duckdb.sql(f"SELECT * FROM read_parquet('{dest}') ORDER BY client_id").fetchall()
    assert got == [("a1", 1, 0), ("b2", 0, 1), ("c3", 1, 1)]
