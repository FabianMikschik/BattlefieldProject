{%- set source_model = "v_stage_server_infos" -%}
{%- set src_pk = "SERVER_MAP_PK" -%}
{%- set src_fk = ["SERVER_PK", "MAP_PK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}