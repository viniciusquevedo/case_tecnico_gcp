# #########################################
# Case Técnico Engenheiro de dados
# #########################################

# Este repositório cobre os quatro pilares:
- Staging / limpeza BigQuery **
- Tabela final de atribuição
- Ingestão + quality gate
- Arquitetura / governança + estratégia de backfill


# 1 - Estrutura do repositório

# sql
- 01_stg_users.sql
- 02_stg_email.sql
- 03_stg_whatsapp.sql
- 04_fct_attribution.sql

# python
- ingestao_emails_logs.py
├─ requirements.txt
└─ README.md

# 2 - Pré requisitos

- BigQuery com 3 tabelas carregadas RAW:
  - raw.raw_sfmc_email_logs
  - raw.raw_whatsapp_provider
  - raw.crm_user_base
  - Python 3.10 ou superior

# 3 - Como rodar

# 3.1 - Rodar o quality gate do Email 
Esse script lê `raw_sfmc_email_logs.csv`, remove linhas com JSON quebrado em `message_details` e gera:
- um CSV limpo
- um log com as linhas rejeitadas

# Depois disso, você pode carregar o arquivo limpo no BigQuery (exemplo com `bq load`):

bq load --source_format=CSV --skip_leading_rows=1 \
  raw.raw_sfmc_email_logs_clean data/clean_sfmc_email_logs.csv \
  event_id:STRING,user_email:STRING,event_timestamp:STRING,event_type:STRING,message_details:STRING


# 3.2 - Criar as tabelas de staging e a fato

Execute os SQLs na ordem:

1) `sql/01_stg_users.sql`  
2) `sql/02_stg_email.sql`  
3) `sql/03_stg_whatsapp.sql`  
4) `sql/04_fct_attribution.sql`

# 4 - Regras aplicadas

# 4.1 - Chaves
- Email: `lower(trim(email))`
- Telefone: apenas dígitos e garantir prefixo 55 quando faltar

# 4.2 - Fuso horario
- Email chega em UTC event_timestamp com Z e é convertido para America/Sao_Paulo antes dos cálculos
- WhatsApp ja chega em BRT e é tratado como America/Sao_Paulo igual o Email

# 4.3 - Parsing de campanha
- Email campaign_code vem do JSON em message_details
- WhatsApp campaign_code vem de campaign_tag

# 4.4 - Atribuição do peso
Interações de até 7 dias antes da conversão

Prioridade menor peso = ganha:
1. WhatsApp `read`
2. Email `click`
3. Email `open`

Se for no mesmo dia:
- Se houver mais de um canal no mesmo dia, o desempate nao vai ser pela hora e sim pelo peso

Fallback:
- O "email sent" e "whatsapp delivered" só entram se não existir nenhuma interação primária read,click,open no período


# 5 - Pilar: Arquitetura e Governança

# 5.1 - Camadas BQ
- Raw: dados brutos exatamente como chegam da API/CSV
- Bronze: dados aceitos após quality gate (JSON válido, schema consistente)
- Silver: staging/modelagem técnica (normalização de chaves, fuso, parsing)
- Gold: fatos e dimensões prontos para consumo `gold.fct_attribution`

# 5.2 - Onde entram procedures
Eu colocaria uma procedure para montar a fct por intervalo, algo como:
- `gold.sp_build_attribution(p_start_date, p_end_date)`

# 5.3 - Backfill (reprocessamento)
Estratégia simples e segura:
- A `gold.fct_attribution` deve ser particionada por `conversion_date`
- Para reprocessar exemplo o último mês:
  deleta apenas as partições do intervalo
  reinsere rodando a mesma lógica
