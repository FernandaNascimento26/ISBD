import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# ============================
#  Configurações iniciais
# ============================
sns.set(style="whitegrid")
diretorio = os.path.join(os.path.dirname(os.path.abspath(__file__)))
quantidade_arquivos = 4  # quantidade de arquivos mais recentes
tempo_maximo_segundos = 120  # limite do gráfico de TPS

# ============================
#  Busca automática dos arquivos mais recentes
# ============================
padrao_data = re.compile(r'benchmark_.*?_(\d{8}_\d{6})\.csv')

def extrair_data(nome):
    m = padrao_data.search(nome)
    if m:
        return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
    return None

arquivos = [
    arq for arq in os.listdir(diretorio)
    if arq.startswith("benchmark_") and arq.endswith(".csv")
]
arquivos.sort(key=lambda x: extrair_data(x) or datetime.min, reverse=True)
arquivos = arquivos[:quantidade_arquivos]

if not arquivos:
    raise FileNotFoundError(f"Nenhum arquivo CSV encontrado no diretório: {diretorio}")

print(f"Arquivos carregados: {arquivos}")

# ============================
#  Carrega e concatena os dados
# ============================
df_list = [pd.read_csv(os.path.join(diretorio, arq)) for arq in arquivos]
df = pd.concat(df_list, ignore_index=True)
df["tempo_execucao"] = pd.to_numeric(df["tempo_execucao"], errors="coerce")
df["timestamp_inicio"] = pd.to_datetime(df["timestamp_inicio"], errors="coerce")

# ============================
#  Cálculo das métricas
# ============================
resposta_media = df.groupby("query")["tempo_execucao"].mean()
resposta_std = df.groupby("query")["tempo_execucao"].std()
execucoes_totais = len(df)
tempo_total_minutos = df["tempo_execucao"].sum() / 60
vazao = execucoes_totais / tempo_total_minutos

# ============================
#  Gráfico 1: Tempo médio por query
# ============================
plt.figure(figsize=(12,6))
sns.barplot(x=resposta_media.index, y=resposta_media.values)
plt.title("Tempo médio de resposta por query")
plt.xlabel("Query")
plt.ylabel("Tempo médio (s)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(diretorio, "grafico_tempo_resposta.png"))
plt.close()

# ============================
#  Gráfico 2: Boxplot dos tempos
# ============================
plt.figure(figsize=(12,6))
sns.boxplot(x="query", y="tempo_execucao", data=df)
plt.title("Distribuição dos tempos de resposta")
plt.xlabel("Query")
plt.ylabel("Tempo (s)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(diretorio, "boxplot_resposta.png"))
plt.close()

# ============================
#  Gráfico 3: Transações por segundo
# ============================
# Normaliza os timestamps
tempo_inicial = df["timestamp_inicio"].min()
df["segundo"] = (df["timestamp_inicio"] - tempo_inicial).dt.total_seconds().astype(int)

# Conta transações por segundo
contagem_segundos = df['segundo'].value_counts().sort_index()
contagem_segundos = contagem_segundos.reindex(range(0, tempo_maximo_segundos + 1), fill_value=0)

plt.figure(figsize=(12,6))
plt.plot(contagem_segundos.index, contagem_segundos.values, marker='o')
plt.title("Número de transações por segundo")
plt.xlabel("Tempo (s)")
plt.ylabel("Transações")
plt.xlim(0, tempo_maximo_segundos)
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(diretorio, "transacoes_por_segundo.png"))
plt.close()

# ============================
#  Saída das métricas
# ============================
print("\nTempo médio por query:\n", resposta_media)
print("\nDesvio padrão por query:\n", resposta_std)
print(f"\nVazão: {vazao:.2f} queries por minuto")
