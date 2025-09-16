from uk_address_matcher.cleaning_v2.pipeline import (
    DuckDBPipeline,
)
from uk_address_matcher.cleaning_v2.cleaning_steps import (
    trim_whitespace_address_and_postcode,
)
import duckdb


con = duckdb.connect()

full_os_path = "secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"

sql = f"""
create or replace table os as
select
   uprn as unique_id,
   'canonical' as source_dataset,
   regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
   postcode,
   latitude as lat,
   longitude as lng
from read_parquet('{full_os_path}')
limit 5

"""
con.execute(sql)
os_df = con.table("os")


pipe = DuckDBPipeline(con, os_df)

pipe.add_step(trim_whitespace_address_and_postcode())

out = pipe.run(pretty_print_sql=True)
print("Result:")
out.show()
