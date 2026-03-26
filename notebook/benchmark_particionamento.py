# =============================================================
# BENCHMARK: Particionamento vs Z-Order vs Liquid Clustering
# Roda no Databricks — DBR 13.3+ recomendado
# Autor: Marcela Monteiro Montenegro Gallo
# =============================================================

# CÉLULA 1 — Configuração
# Cole cada bloco em uma célula separada no notebook Databricks

CATALOG  = "main"
SCHEMA   = "benchmark_demo"
N_ROWS   = 50_000_000   # 50M linhas (~8GB) — ajuste conforme cluster
N_CLIENTES = 500_000

spark.conf.set("spark.sql.shuffle.partitions", "200")

print(f"Gerando {N_ROWS:,} linhas com {N_CLIENTES:,} clientes distintos")
print(f"Catalog: {CATALOG}.{SCHEMA}")


# =============================================================
# CÉLULA 2 — Gera massa de dados
# =============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Gera DataFrame com dados realistas de vendas
df_raw = (
    spark.range(N_ROWS)
    .withColumn("venda_id",    F.col("id"))
    .withColumn("cliente_id",  (F.rand() * N_CLIENTES).cast("long"))
    .withColumn("produto_id",  (F.rand() * 10_000).cast("long"))
    .withColumn("regiao",      F.element_at(
        F.array(F.lit("sudeste"), F.lit("sul"), F.lit("nordeste"),
                F.lit("norte"),   F.lit("centro-oeste")),
        (F.rand() * 5 + 1).cast("int")
    ))
    .withColumn("data_venda",  (
        F.date_add(F.lit("2022-01-01"),
                   (F.rand() * 730).cast("int"))
    ))
    .withColumn("valor",       (F.rand() * 5000 + 10).cast("decimal(18,2)"))
    .withColumn("status",      F.element_at(
        F.array(F.lit("aprovado"), F.lit("cancelado"), F.lit("pendente")),
        (F.rand() * 3 + 1).cast("int")
    ))
    .withColumn("canal",       F.element_at(
        F.array(F.lit("app"), F.lit("web"), F.lit("loja"), F.lit("telefone")),
        (F.rand() * 4 + 1).cast("int")
    ))
    .drop("id")
)

# Persiste em memória para reutilizar nas 4 tabelas
df_raw.cache()
count = df_raw.count()
print(f"✅ {count:,} linhas geradas")


# =============================================================
# CÉLULA 3 — Cria as 4 versões da tabela
# =============================================================

BASE = f"{CATALOG}.{SCHEMA}"

# --- Tabela 1: Sem otimização ---
print("Criando tabela 1/4: sem_otimizacao...")
spark.sql(f"DROP TABLE IF EXISTS {BASE}.vendas_sem_otimizacao")
df_raw.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BASE}.vendas_sem_otimizacao")
print("  ✅ vendas_sem_otimizacao criada")

# --- Tabela 2: Particionada por data_venda ---
print("Criando tabela 2/4: particionada_data...")
spark.sql(f"DROP TABLE IF EXISTS {BASE}.vendas_particionada_data")
df_raw.write.format("delta").mode("overwrite") \
    .partitionBy("data_venda") \
    .saveAsTable(f"{BASE}.vendas_particionada_data")
print("  ✅ vendas_particionada_data criada")

# --- Tabela 3: Z-Order por cliente_id ---
print("Criando tabela 3/4: zorder_cliente...")
spark.sql(f"DROP TABLE IF EXISTS {BASE}.vendas_zorder_cliente")
df_raw.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BASE}.vendas_zorder_cliente")
spark.sql(f"OPTIMIZE {BASE}.vendas_zorder_cliente ZORDER BY (cliente_id)")
print("  ✅ vendas_zorder_cliente criada + OPTIMIZE ZORDER aplicado")

# --- Tabela 4: Liquid Clustering ---
print("Criando tabela 4/4: liquid_clustering...")
spark.sql(f"DROP TABLE IF EXISTS {BASE}.vendas_liquid_clustering")
spark.sql(f"""
    CREATE TABLE {BASE}.vendas_liquid_clustering
    CLUSTER BY (cliente_id, data_venda)
    AS SELECT * FROM {BASE}.vendas_sem_otimizacao
""")
spark.sql(f"OPTIMIZE {BASE}.vendas_liquid_clustering")
print("  ✅ vendas_liquid_clustering criada + OPTIMIZE aplicado")

print("\n🎯 Todas as 4 tabelas prontas!")


# =============================================================
# CÉLULA 4 — Função de benchmark com métricas reais
# =============================================================
import re

def run_benchmark(table_name, query_sql, label):
    """Roda a query e captura tempo + arquivos lidos via EXPLAIN e métricas."""

    # Limpa cache para medição justa
    spark.catalog.clearCache()
    spark.sql("CLEAR CACHE")

    # Executa e mede tempo
    start = time.time()
    result = spark.sql(query_sql)
    count  = result.count()
    elapsed = time.time() - start

    # Pega métricas de arquivos via DESCRIBE HISTORY / operationMetrics
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
        metrics = history.select("operationMetrics").first()[0] or {}
        files_read = metrics.get("numFiles", "N/A")
    except:
        files_read = "N/A"

    # Alternativa: usa EXPLAIN para estimar arquivos
    explain_output = spark.sql(f"EXPLAIN COST {query_sql}").collect()
    explain_text   = " ".join([str(r) for r in explain_output])
    files_match    = re.findall(r'numFiles=(\d+)', explain_text)
    files_pruned   = files_match[0] if files_match else "ver Spark UI"

    mins  = int(elapsed // 60)
    secs  = elapsed % 60

    print(f"{'─'*55}")
    print(f"  Estratégia : {label}")
    print(f"  Tabela     : {table_name}")
    print(f"  Linhas     : {count:,}")
    print(f"  Tempo      : {mins}m {secs:.1f}s")
    print(f"  Arquivos   : {files_pruned}")
    print(f"{'─'*55}")

    return {"label": label, "tempo_s": elapsed, "linhas": count,
            "mins": mins, "secs": secs}


# =============================================================
# CÉLULA 5 — Roda o benchmark nas 4 tabelas
# Escolhe um cliente_id real para garantir resultado
# =============================================================

# Pega um cliente_id que existe nos dados
sample_cliente = spark.sql(f"""
    SELECT cliente_id, COUNT(*) as qtd
    FROM {BASE}.vendas_sem_otimizacao
    GROUP BY cliente_id
    ORDER BY qtd DESC
    LIMIT 1
""").first()["cliente_id"]

print(f"🔍 Testando com cliente_id = {sample_cliente}")
print(f"{'='*55}")

QUERY_TEMPLATE = """
    SELECT
        cliente_id,
        COUNT(*)            AS total_vendas,
        SUM(valor)          AS total_valor,
        AVG(valor)          AS ticket_medio,
        MIN(data_venda)     AS primeira_compra,
        MAX(data_venda)     AS ultima_compra
    FROM {table}
    WHERE cliente_id = {cliente}
    GROUP BY cliente_id
"""

resultados = []

# 1. Sem otimização
r = run_benchmark(
    f"{BASE}.vendas_sem_otimizacao",
    QUERY_TEMPLATE.format(table=f"{BASE}.vendas_sem_otimizacao", cliente=sample_cliente),
    "Sem otimização"
)
resultados.append(r)

# 2. Particionada por data (pior caso — filtro não usa partição)
r = run_benchmark(
    f"{BASE}.vendas_particionada_data",
    QUERY_TEMPLATE.format(table=f"{BASE}.vendas_particionada_data", cliente=sample_cliente),
    "Particionado por data"
)
resultados.append(r)

# 3. Z-Order
r = run_benchmark(
    f"{BASE}.vendas_zorder_cliente",
    QUERY_TEMPLATE.format(table=f"{BASE}.vendas_zorder_cliente", cliente=sample_cliente),
    "Z-Order por cliente_id"
)
resultados.append(r)

# 4. Liquid Clustering
r = run_benchmark(
    f"{BASE}.vendas_liquid_clustering",
    QUERY_TEMPLATE.format(table=f"{BASE}.vendas_liquid_clustering", cliente=sample_cliente),
    "Liquid Clustering"
)
resultados.append(r)


# =============================================================
# CÉLULA 6 — Tabela de resultados formatada para print/screenshot
# =============================================================

print("\n")
print("╔══════════════════════════════════════════════════════════════╗")
print("║         BENCHMARK — Particionamento no Databricks           ║")
print("║         Filtro: cliente_id (alta cardinalidade)             ║")
print("╠══════════════════════════════════════════════════════════════╣")
print(f"║  {'Estratégia':<28} {'Tempo':>10}  {'Melhoria':>10}  ║")
print("╠══════════════════════════════════════════════════════════════╣")

base_tempo = resultados[0]["tempo_s"]
for r in resultados:
    melhoria = f"{base_tempo / r['tempo_s']:.1f}x" if r['tempo_s'] > 0 else "—"
    tempo_fmt = f"{r['mins']}m {r['secs']:.1f}s"
    print(f"║  {r['label']:<28} {tempo_fmt:>10}  {melhoria:>10}  ║")

print("╚══════════════════════════════════════════════════════════════╝")

# Redução percentual do Liquid Clustering vs sem otimização
lc_tempo   = resultados[3]["tempo_s"]
sem_tempo  = resultados[0]["tempo_s"]
reducao    = (1 - lc_tempo / sem_tempo) * 100
print(f"\n🏆 Liquid Clustering foi {reducao:.1f}% mais rápido que sem otimização")
print(f"   ({resultados[0]['mins']}m{resultados[0]['secs']:.0f}s → {resultados[3]['mins']}m{resultados[3]['secs']:.0f}s)")

# =============================================================
# CÉLULA 7 — Conta arquivos reais por tabela (para o print)
# =============================================================
print("\n📁 Arquivos físicos por tabela:")
print("─" * 45)
for tbl, label in [
    ("vendas_sem_otimizacao",    "Sem otimização"),
    ("vendas_particionada_data", "Particionado por data"),
    ("vendas_zorder_cliente",    "Z-Order"),
    ("vendas_liquid_clustering", "Liquid Clustering"),
]:
    detail = spark.sql(f"DESCRIBE DETAIL {BASE}.{tbl}").first()
    n_files = detail["numFiles"]
    size_gb = detail["sizeInBytes"] / (1024**3)
    print(f"  {label:<28} {n_files:>6} arquivos  {size_gb:.1f} GB")

# =============================================================
# CÉLULA 8 — Query adicional: mostra data skipping em ação
# Roda no Spark UI para ver "files pruned" vs "files read"
# =============================================================
print("\n\n📊 Para ver data skipping no Spark UI:")
print("   Roda a query abaixo e abre: Spark UI → SQL → último job")
print("   Procura por 'number of files pruned' nas métricas\n")

for tbl, label in [
    ("vendas_sem_otimizacao",    "Sem otimização"),
    ("vendas_liquid_clustering", "Liquid Clustering"),
]:
    print(f"-- {label}")
    print(f"SELECT COUNT(*), SUM(valor)")
    print(f"FROM {BASE}.{tbl}")
    print(f"WHERE cliente_id = {sample_cliente};")
    print()
