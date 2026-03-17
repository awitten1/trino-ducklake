Regenerate the test DuckLake database from scratch.

This deletes the existing metadata.sqlite and data_files/ directory, then re-runs the generator script to produce a fresh database with table `x` (2000 rows, 100 deleted via positional delete file).

```bash
rm -f metadata.sqlite && rm -rf data_files/ && ./generate-ducklake-database.sh
```
