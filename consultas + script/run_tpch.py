import psycopg2
import time
import csv
from datetime import datetime

# Configurações de conexão
conn_params = {
    "host": "localhost",
    "port": 5433,
    "dbname": "tpch",
    "user": "postgres",
    "password": "nandan24"
}

# Diretório das queries limpas
query_dir = "queries"

# Lista para armazenar os resultados
benchmark_results = []

# Tempo total de execução (em segundos)
tempo_total = 1800  # 30 minutos.

# Conectar ao banco
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

# Início do benchmark
start_global = time.time()
execucoes = 0

while time.time() < start_global + tempo_total:
    for qnum in range(1, 23):  # Q1 a Q22
        query_path = f"{query_dir}/{qnum}.sql"
        with open(query_path, "r", encoding="utf-8") as f:
            query_text = f.read()

        timestamp_inicio = datetime.now().isoformat()
        start_time = time.time()

        try:
            cursor.execute(query_text)
            cursor.fetchall()  # força execução completa
        except Exception as e:
            print(f"Erro na Q{qnum}: {e}")
            continue

        end_time = time.time()
        timestamp_fim = datetime.now().isoformat()
        tempo_execucao = round(end_time - start_time, 3)

        benchmark_results.append([
            "TPC-H", f"Q{qnum}", execucoes + 1, tempo_execucao, timestamp_inicio, timestamp_fim
        ])

        print(f"Q{qnum} | Execução {execucoes + 1} | Tempo: {tempo_execucao}s")

    execucoes += 1
    tempo_passado = round(time.time() - start_global, 1)
    tempo_restante = round((start_global + tempo_total) - time.time(), 1)
    print(f"\n⏱️ Execução {execucoes} concluída | Tempo total: {tempo_passado}s | Restante: {tempo_restante}s\n")

# Fechar conexão
cursor.close()
conn.close()

# Salvar em CSV
csv_path = "benchmark_tpch_temporizado.csv"
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["benchmark", "query", "n", "tempo_execucao", "timestamp_inicio", "timestamp_fim"])
    writer.writerows(benchmark_results)

print(f"\n✅ Benchmark finalizado. {execucoes} execuções completas. Resultados salvos em: {csv_path}")