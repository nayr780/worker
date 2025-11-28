#!/usr/bin/env python3
import asyncio
from arq.connections import create_pool, RedisSettings
from main import pick_queue_for_colab  # usa sua própria lógica
import re

# =========================================================
# CONFIG REDIS WS (Render)
# =========================================================
REDIS_WS_URL = "wss://redisrender.onrender.com"
REDIS_TOKEN = ""  # se tiver token, coloque aqui

# Conexão ao PROXY local (igual o worker usa)
# Você deve deixar o proxy rodando ou apontar para o proxy remoto
REDIS_SETTINGS = RedisSettings(
    host="127.0.0.1",
    port=6380,
    database=0,
    ssl=False,
)

# =========================================================
# 🔧 CONFIG JOBS
# =========================================================
USUARIO = "44036000349"
SENHA = "ISSavancar2024**"
MES = "11/2025"

COLABS = ["joao", "carlos", "jair"]
BLOCO = 4  # 4 cnpjs para cada

# =========================================================
# HELPERS
# =========================================================
def limpar_cnpj(cnpj):
    digitos = re.sub(r"\D", "", str(cnpj))
    return digitos[-14:].zfill(14)

# =========================================================
# FUNÇÃO PRINCIPAL
# =========================================================
async def main():
    redis = await create_pool(REDIS_SETTINGS)

    # Exemplo de CNPJs
    cnps = [
        "12.345.678/0001-99"    ]

    for idx, raw in enumerate(cnps):
        cnpj = limpar_cnpj(raw)
        if len(cnpj) != 14:
            print(f"[ERRO] CNPJ inválido: {raw}")
            continue

        colab_norm = COLABS[(idx // BLOCO) % len(COLABS)]
        queue = pick_queue_for_colab(colab_norm)

        print("\n==============================")
        print(f"ENFILEIRANDO CNPJ {cnpj} → COLAB {colab_norm}")
        print("==============================")

        # 1) job_notas
        job1 = await redis.enqueue_job(
            "job_notas",
            colab_norm,
            cnpj,
            MES,
            USUARIO,
            SENHA,
            "convencional",
            _queue_name=queue,
        )
        print("✔ job_notas:", job1.job_id)

        # 2) job_certidao
        job2 = await redis.enqueue_job(
            "job_certidao",
            colab_norm,
            cnpj,
            MES,
            USUARIO,
            SENHA,
            "convencional",
            _queue_name=queue,
        )
        print("✔ job_certidao:", job2.job_id)

    await redis.aclose()
    print("\n===== ENFILEIRAMENTO CONCLUÍDO =====")

# =========================================================
if __name__ == "__main__":
    asyncio.run(main())
