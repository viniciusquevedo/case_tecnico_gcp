-- 03_stg_whatsapp.sql
-- Logs do provedor de WhatsApp utc am/sp, parsing do campaign_code via regex

create or replace table silver.stg_whatsapp_events as
with src as (
  select
    cast(message_id as string) as message_id,
    regexp_replace(cast(phone_clean as string), r'\D', '') as phone_norm,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', cast(sent_at_brt as string)) as event_dt_sp,
    lower(trim(cast(status as string))) as status,
    cast(campaign_tag as string) as campaign_tag
  from raw.raw_whatsapp_provider
)
select
  message_id,
  phone_norm,
  event_dt_sp,
  status,
  regexp_extract(campaign_tag, r'\[WA\]\s*-\s*([A-Z0-9_]+)\s*-') as campaign_code
from src
where event_dt_sp is not null;
