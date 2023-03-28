{%- set yaml_metadata -%}
source_model: 'raw_server_bridge_players'
derived_columns:
  RECORD_SOURCE: '!raw_server_infos'
hashed_columns:
  SERVER_PK: 'server_id'
  PLAYER_PK: 'player_id'
  SERVER_PLAYERS_PK:
    - 'server_id'
    - 'player_id'
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