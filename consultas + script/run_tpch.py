import psycopg2
import time
import csv
from datetime import datetime
import threading
import os

# Configurações de conexão
conn_params = {
    "host": "localhost",
    "port": 5433,
    "dbname": "tpch",
    "user": "postgres",
    "password": "nandan24"
}

query_dir = "queries"
tempo_total = 1800  # 30 minutos

def executar_benchmark(usuario_id):
    benchmark_results = []
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
    except Exception as e:
        print(f"[User {usuario_id}]  Erro ao conectar: {e}")
        return

    start_global = time.time()
    execucoes = 0

    print(f"\n [User {usuario_id}] Iniciando benchmark por {tempo_total}s...\n")

    while time.time() < start_global + tempo_total:
        for qnum in range(1, 23):
            query_path = f"{query_dir}/{qnum}.sql"
            try:
                with open(query_path, "r", encoding="utf-8") as f:
                    query_text = f.read()
            except Exception as e:
                print(f"[User {usuario_id}] Erro ao ler Q{qnum}: {e}")
                continue

            timestamp_inicio = datetime.now().isoformat()
            start_time = time.time()

            try:
                cursor.execute(query_text)
                cursor.fetchall()
            except Exception as e:
                print(f"[User {usuario_id}] Erro na Q{qnum}: {e}")
                conn.rollback()
                continue

            end_time = time.time()
            timestamp_fim = datetime.now().isoformat()
            tempo_execucao = round(end_time - start_time, 3)

            benchmark_results.append([
                "TPC-H", f"Q{qnum}", execucoes + 1, tempo_execucao, timestamp_inicio, timestamp_fim
            ])

            print(f"[User {usuario_id}]  Q{qnum} | Execução {execucoes + 1} | Tempo: {tempo_execucao}s")

        execucoes += 1
        tempo_passado = round(time.time() - start_global, 1)
        tempo_restante = round((start_global + tempo_total) - time.time(), 1)
        print(f"\n [User {usuario_id}] Execução {execucoes} concluída | Tempo total: {tempo_passado}s | Restante: {tempo_restante}s\n")

    cursor.close()
    conn.close()

    csv_path = os.path.join(os.getcwd(), f"benchmark_tpch_user{usuario_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["benchmark", "query", "n", "tempo_execucao", "timestamp_inicio", "timestamp_fim"])
            writer.writerows(benchmark_results)
        print(f"\n[User {usuario_id}] Benchmark finalizado. Resultados salvos em: {csv_path}")
    except Exception as e:
        print(f"[User {usuario_id}] Falha ao salvar CSV: {e}")

# Criar e iniciar 4 threads
threads = []
for i in range(1, 5):
    t = threading.Thread(target=executar_benchmark, args=(i,))
    threads.append(t)
    t.start()

# Esperar todas terminarem
for t in threads:
    t.join()

print("\n🏁 Todos os usuários concluíram o benchmark.")
