{{ config(materialized="view") }}


select * from {{ ref("bar") }}
