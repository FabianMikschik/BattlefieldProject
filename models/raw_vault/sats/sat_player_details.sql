{%- set source_model = "v_stage_server_players" -%}
{%- set src_pk = "PLAYER_PK" -%}
{%- set src_hashdiff = "PLAYER_HASHDIFF" -%}
{%- set src_payload = ["rank", "score", "kills", "deaths", "squad", "role", "name"] -%}
{%- set src_eff = "LOAD_DATE" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
