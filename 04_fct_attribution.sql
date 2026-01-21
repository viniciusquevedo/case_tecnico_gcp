-- 04_fct_attribution.sql
-- Tabela final: "Qual campanha gerou esta conversão?"
-- Regra: janela 7 dias, Weighted Last Touch (mesmo dia desempata pelo peso)
-- Pesos:
--  1 = whatsapp read
--  2 = email click
--  3 = email open
--  99 = Cai no fallback (email sent / whatsapp delivered) só se não existir interação primaria

create or replace table gold.fct_attribution
partition by conversion_date
cluster by user_id, attributed_campaign_code as
with conv as (
  select
    user_id,
    email_norm,
    phone_norm,
    conversion_dt_sp,
    conversion_date
  from silver.stg_users
  where conversion_dt_sp is not null
),
email_interactions as (
  select
    c.user_id,
    c.conversion_dt_sp,
    e.event_dt_sp as interaction_dt_sp,
    date(e.event_dt_sp) as interaction_date,
    'email' as channel,
    e.campaign_code,
    e.event_type as interaction_type,
    case
      when e.event_type = 'click' then 2
      when e.event_type = 'open'  then 3
      when e.event_type = 'sent'  then 99
      else 99
    end as weight,
    case when e.event_type in ('click','open') then 1 else 0 end as is_primary
  from conv c
  join silver.stg_email_events e
    on e.email_norm = c.email_norm
  where e.event_dt_sp between datetime_sub(c.conversion_dt_sp, interval 7 day) and c.conversion_dt_sp
    and e.campaign_code is not null
),
whats_interactions as (
  select
    c.user_id,
    c.conversion_dt_sp,
    w.event_dt_sp as interaction_dt_sp,
    date(w.event_dt_sp) as interaction_date,
    'whatsapp' as channel,
    w.campaign_code,
    w.status as interaction_type,
    case
      when w.status = 'read'      then 1
      when w.status = 'delivered' then 99
      else null
    end as weight,
    case when w.status = 'read' then 1 else 0 end as is_primary
  from conv c
  join silver.stg_whatsapp_events w
    on w.phone_norm = c.phone_norm
  where w.event_dt_sp between datetime_sub(c.conversion_dt_sp, interval 7 day) and c.conversion_dt_sp
    and w.status in ('read','delivered')
    and w.campaign_code is not null
),
all_interactions as (
  select * from email_interactions
  union all
  select * from whats_interactions
),
scored as (
  select
    *,
    max(is_primary) over(partition by user_id, conversion_dt_sp) as has_primary_in_window
  from all_interactions
),
eligible as (
  select *
  from scored
  where (has_primary_in_window = 1 and is_primary = 1)
     or (has_primary_in_window = 0 and is_primary = 0)
),
winner as (
  select
    user_id,
    conversion_dt_sp,
    channel,
    campaign_code,
    interaction_type,
    interaction_dt_sp,
    weight,
    row_number() over(
      partition by user_id, conversion_dt_sp
      order by interaction_date desc, weight asc, interaction_dt_sp desc
    ) as rn
  from eligible
)
select
  c.user_id,
  c.conversion_dt_sp as conversion_at_sp,
  c.conversion_date,
  w.channel as attributed_channel,
  w.campaign_code as attributed_campaign_code,
  w.interaction_type as attributed_interaction_type,
  w.interaction_dt_sp as attributed_interaction_at_sp,
  w.weight as attributed_weight,
  case when w.user_id is null then 0 else 1 end as fl_has_attribution
from conv c
left join winner w
  on w.user_id = c.user_id
 and w.conversion_dt_sp = c.conversion_dt_sp
 and w.rn = 1;
