{{ config(materialized="view") }}

SELECT * FROM {{source("raw", "landing_server_bridge_players")}}