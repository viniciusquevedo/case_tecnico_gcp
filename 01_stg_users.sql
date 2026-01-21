-- 01_stg_users.sql
-- Base de usuários dimensao com chaves normalizadas e conversão em datetime am/sp

create or replace table silver.stg_users as
with src as (
  select
    cast(user_id as string) as user_id,
    lower(trim(email)) as email_norm,
    regexp_replace(cast(phone as string), r'\D', '') as phone_digits,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', cast(conversion_at as string)) as conversion_dt_sp
  from raw.crm_user_base
)
select
  user_id,
  email_norm,
  case
    when starts_with(phone_digits, '55') then phone_digits
    when phone_digits is null or phone_digits = '' then null
    else concat('55', phone_digits)
  end as phone_norm,
  conversion_dt_sp,
  date(conversion_dt_sp) as conversion_date
from src;
