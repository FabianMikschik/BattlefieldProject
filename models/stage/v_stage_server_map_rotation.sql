{%- set yaml_metadata -%}
source_model: 'raw_server_map_rotation'
derived_columns:
  RECORD_SOURCE: '!raw_server_map_rotation'
hashed_columns:
  MAP_PK: 'mapname'
  MAP_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'mapname'
      - 'image'
      - 'mode'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *,
       {{ dbt_date.now() }} AS LOAD_DATE
FROM staging