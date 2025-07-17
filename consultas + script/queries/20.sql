import psycopg2
import time
import csv
from datetime import datetime
import threading

# Configurações de conexão
conn_params = {
    "host": "localhost",
    "port": 5433,
    "dbname": "tpch",
    "user": "postgres",
    "password": "sua_senha_aqui"
}

query_dir = "queries_limpo"
tempo_total = 60  # segundos

def executar_benchmark(usuario_id):
    benchmark_results = []
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    start_global = time.time()
    execucoes = 0

    print(f"\n🚀 [User {usuario_id}] Iniciando benchmark por {tempo_total}s...\n")

    while time.time() < start_global + tempo_total:
        exec_start = time.time()
        for qnum in range(1, 23):
            query_path = f"{query_dir}/{qnum}.sql"
            with open(query_path, "r", encoding="utf-8") as f:
                query_text = f.read()

            timestamp_inicio = datetime.now().isoformat()
            start_time = time.time()

            try:
                cursor.execute(query_text)
                cursor.fetchall()
            except Exception as e:
                print(f"[User {usuario_id}] ❌ Erro na Q{qnum}: {e}")
                continue

            end_time = time.time()
            timestamp_fim = datetime.now().isoformat()
            tempo_execucao = round(end_time - start_time, 3)

            benchmark_results.append([
                "TPC-H", f"Q{qnum}", execucoes + 1, tempo_execucao, timestamp_inicio, timestamp_fim
            ])

            print(f"[User {usuario_id}] ✅ Q{qnum} | Execução {execucoes + 1} | Tempo: {tempo_execucao}s")

        exec_end = time.time()
        tempo_execucao_total = round(exec_end - exec_start, 2)
        tempo_passado = round(exec_end - start_global, 1)
        tempo_restante = round((start_global + tempo_total) - exec_end, 1)

        print(f"\n⏱️ [User {usuario_id}] Execução {execucoes + 1} concluída")
        print(f"[User {usuario_id}] Tempo total da execução: {tempo_execucao_total}s")
        print(f"[User {usuario_id}] Tempo total passado: {tempo_passado}s | Restante: {tempo_restante}s\n")

        execucoes += 1

    cursor.close()
    conn.close()

    csv_path = f"benchmark_tpch_user{usuario_id}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["benchmark", "query", "n", "tempo_execucao", "timestamp_inicio", "timestamp_fim"])
        writer.writerows(benchmark_results)

    print(f"\n[User {usuario_id}] Benchmark finalizado. {execucoes} execuções completas.")
    print(f"[User {usuario_id}] Resultados salvos em: {csv_path}\n")

# Criar e iniciar 4 threads
threads = []
for i in range(1, 5):
    t = threading.Thread(target=executar_benchmark, args=(i,))
    threads.append(t)
    t.start()

# Esperar todas terminarem
for t in threads:
    t.join()

print("\nTodos os usuários concluíram o benchmark.")