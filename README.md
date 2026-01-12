# Medallion Pipeline

## Visão Geral do Projeto

Este projeto implementa um **pipeline de dados ETL** seguindo a **arquitetura Medallion** (Bronze → Silver → Gold), padrão amplamente adotado em plataformas de dados modernas como Databricks e Delta Lake.

---

## Decisões Arquiteturais e Justificativas

### 1. Arquitetura Medallion (3 camadas)

| Camada | Propósito | Justificativa |
|--------|-----------|---------------|
| **Bronze** | Dados brutos + metadados de auditoria | Preserva origem exata para rastreabilidade e reprocessamento |
| **Silver** | Dados limpos e validados | Separa lógica de limpeza, facilita manutenção e debugging |
| **Gold** | Modelos analíticos (dimensões/fatos) | Pronto para consumo por dashboards e análises |

**Vantagem:** Cada camada tem responsabilidade única (Single Responsibility), facilitando debugging e evolução independente.

---

### 2. Particionamento por `ingest_date`

- Dados são organizados em pastas `ingest_date=YYYY-MM-DD`
- Permite reprocessamento seletivo de períodos específicos
- Simula padrão de data lakes (Hive-style partitioning)

---

### 3. Metadados de Linhagem (5 colunas adicionadas na Bronze)

```
_source_file_folder, _source_file_name, _source_file_ingest_date,
_source_file_modified_ts, _processed_ts
```

**Justificativa:** Rastreabilidade completa — permite identificar exatamente de onde cada registro veio e quando foi processado.

---

### 4. Tratamento de Qualidade de Dados (Silver)

| Problema | Solução |
|----------|---------|
| Estados inválidos | Validação contra lista oficial de 27 UFs |
| Telefones malformados | Regex + validação de 10-11 dígitos |
| Valores monetários BR | Conversão `2.026,00` → `2026.00` |
| Quantidades em texto | Mapeamento `"two"` → `2` |
| Datas inconsistentes | `delivered_ts < shipped_ts` → limpa ambas |
| Duplicatas | Deduplicação por ID mantendo versão mais recente |

---

### 5. Modelagem Dimensional (Gold)

- **Dimensões:** `dim_customers`, `dim_products` — entidades de negócio
- **Fatos:** `fact_orders`, `fact_order_items` — eventos transacionais
- **Métricas calculadas:** `gross_amount`, `discount_total`, `net_amount`, `delivery_time_hours`, `is_late`

**Vantagem:** Modelo star schema facilita análises agregadas e integração com ferramentas de BI.

---

### 6. Tratamento de Nulos por Tipo

| Tipo de Dado | Valor Nulo |
|--------------|------------|
| Datetime | `pd.NaT` |
| Float/Numérico | `np.nan` |
| String/Boolean | `pd.NA` |

**Justificativa:** Evita inconsistências de tipo e comportamentos inesperados em operações pandas.

---

### 7. Valores Monetários com 2 Casas Decimais

Todos os cálculos financeiros usam `.round(2)` para evitar erros de precisão de ponto flutuante.

---

## Vantagens da Abordagem

1. **Reprodutibilidade:** Dados brutos preservados permitem reprocessamento completo
2. **Debuggabilidade:** Cada camada pode ser inspecionada isoladamente
3. **Extensibilidade:** Novas tabelas/colunas adicionadas sem alterar estrutura existente
4. **Rastreabilidade:** Metadados de linhagem permitem auditoria completa
5. **Qualidade:** Validações específicas por domínio (UFs, telefones, datas)
6. **Padrão de Mercado:** Arquitetura Medallion é amplamente reconhecida e documentada

---

## Aspectos Técnicos Importantes

- **Segurança:** Função `clean_directory()` valida que só limpa dentro de `output/`
- **Delay entre camadas:** 3 segundos para garantir sincronização de filesystem
- **Erro handling:** Try/except no `main.py` captura falhas sem expor stack trace
- **Funções atômicas:** Cada função de limpeza trata um único tipo de dado

---
