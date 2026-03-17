#!/bin/bash

set -eux

duckdb -c "
install ducklake; load ducklake;
attach 'ducklake:sqlite:metadata.sqlite' as my_ducklake (DATA_PATH 'data_files/');
use my_ducklake;
create or replace table x as select i as col1, i**2 as col2 from range(1000) r(i);
insert into x select i + 2000, null from range(1000) r(i);
delete from x where col1 < 100;
checkpoint;
"
