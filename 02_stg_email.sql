-- 02_stg_email.sql
-- Logs de Email utc am/sp, parsing do campaign_code JSON

create or replace table silver.stg_email_events as
with src as (
  select
    cast(event_id as string) as event_id,
    lower(trim(user_email)) as email_norm,
    safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', cast(event_timestamp as string)) as ts_utc,
    lower(trim(cast(event_type as string))) as event_type,
    cast(message_details as string) as message_details
  from raw.raw_sfmc_email_logs
)
select
  event_id,
  email_norm,
  datetime(ts_utc, 'America/Sao_Paulo') as event_dt_sp,
  event_type,
  json_value(safe.parse_json(message_details), '$.campaign_code') as campaign_code
from src
where ts_utc is not null;
