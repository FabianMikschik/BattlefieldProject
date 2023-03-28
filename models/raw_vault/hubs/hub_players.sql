{%- set source_model = "v_stage_server_players" -%}
{%- set src_pk = "PLAYER_PK" -%}
{%- set src_nk = "player_id" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}