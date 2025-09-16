from uk_address_matcher.cleaning_v2.pipeline import (
    DuckDBPipeline,
)
from uk_address_matcher.cleaning_v2.cleaning_steps import (
    canonicalise_postcode,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    derive_original_address_concat,
    parse_out_flat_position_and_letter,
    parse_out_numbers,
    remove_duplicate_end_tokens,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
)
import duckdb
import time


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
limit 50000

"""
con.execute(sql)
os_df = con.table("os")

start_time = time.time()

pipe = DuckDBPipeline(con, os_df)

pipe.add_step(trim_whitespace_address_and_postcode())
pipe.add_step(canonicalise_postcode())
pipe.add_step(upper_case_address_and_postcode())
pipe.add_step(clean_address_string_first_pass())
pipe.add_step(remove_duplicate_end_tokens())
pipe.add_step(derive_original_address_concat())
pipe.add_step(parse_out_flat_position_and_letter())
pipe.add_step(parse_out_numbers())
pipe.add_step(clean_address_string_second_pass())
pipe.add_step(split_numeric_tokens_to_cols())
pipe.add_step(tokenise_address_without_numbers())

out = pipe.run(pretty_print_sql=False)
print("Result:")
out.df()

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")

# TODO: Before continuing migration, add test that checks result is identical to old pipeline
