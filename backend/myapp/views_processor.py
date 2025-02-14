"""Módulo com classes para processamento de dados antes de serem enviados para o frontend"""

import numpy as np
import pandas as pd

from .utils import PESO_BANDEJAS, PESO_SACO


class QualidadeDataProcessor:
    "Processa os dados de qualidade antes de serem enviados para o frontend"

    @staticmethod
    def process_qualidade_data(df: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados de qualidade antes de serem enviados para o frontend"""

        # Colunas para arredondamento
        round_columns = [
            "bdj_vazias",
            "bdj_retrabalho",
            "descarte_paes",
            "descarte_paes_pasta",
            "descarte_pasta",
        ]

        # Arredonda valores
        for col in round_columns:
            df[col] = df[col].round(3)

        # Calcula bandejas
        for col in ["bdj_vazias", "bdj_retrabalho"]:
            mask = df[col] > 0
            df.loc[mask, col] = ((df[col] - PESO_SACO) / PESO_BANDEJAS).round(0)
            df[col] = df[col].astype(int).clip(lower=0)

        return df


class ProductionDataProcessor:
    """
    Processa os dados de produção antes de serem enviados para o frontend.

    Principais funcionalidades:
    - Processamento de data/hora
    - Agregação por hora
    - Cálculo de totais produzidos
    - Conversão de bandejas para caixas
    - Formatação final dos dados
    """

    @staticmethod
    def process_production_data(df: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados de produção antes de serem enviados para o frontend"""

        # Ajuste de data
        df.data_registro = pd.to_datetime(df.data_registro)
        df.data_registro = df.data_registro.dt.strftime("%d/%m/%Y")

        # Se houver valor nulo de maquina_id remove
        df = df.dropna(subset=["maquina_id"])

        # Cria uma coluna auxiliar com data_hora
        df["data_hora"] = (
            df.data_registro + " " + df.hora_registro.astype(str).str.split(".").str[0]
        )

        # # Ajusta data_hora
        # df.data_hora = pd.to_datetime(df.data_hora)

        # Ajusta data_hora - Corrigido o parsing da data
        df.data_hora = pd.to_datetime(df.data_hora, format="%d/%m/%Y %H:%M:%S")

        # Torna a data e a maquina_id como índice
        df = df.set_index(["data_hora", "maquina_id"])

        # Agrupa os dados
        df = (
            df.groupby("maquina_id")
            .resample("h", level="data_hora")
            .agg(
                {
                    "contagem_total_produzido": ["first", "last"],
                    "contagem_total_ciclos": ["first", "last"],
                }
            )
            .reset_index()
        )

        # Calcula a diferença entre o primeiro e o último valor
        df["total_produzido"] = (
            df["contagem_total_produzido"]["last"] - df["contagem_total_produzido"]["first"]
        )
        df["total_ciclos"] = (
            df["contagem_total_ciclos"]["last"] - df["contagem_total_ciclos"]["first"]
        )

        # Ajuste para compensar possível falha do sensor de contagem de pão
        mask = (df.total_ciclos - df.total_produzido) / df.total_ciclos > 0.25
        df["total"] = np.where(mask, df.total_ciclos, df.total_produzido)

        # Manter apenas o necessário
        df = df[["data_hora", "total", "maquina_id"]]

        # O total deve ser no mínimo 0
        df.total = df.total.clip(lower=0)

        # Preencher valores nulos com 0
        df.loc[:, "total"] = df.total.fillna(0)

        # Transformar de bandejas para caixas
        df.loc[:, "total"] = np.floor(df.total / 10).astype(int)

        # Pivotar a tabela
        df = df.pivot(index="data_hora", columns="maquina_id", values="total")

        # Preencher valores nulos com 0
        df = df.fillna(0)
        df = df.astype(int)

        # Valor mínimo é 0
        df = df.clip(lower=0)

        # Criar uma coluna com o intervalo de tempo
        df["intervalo"] = (
            df.index.hour.astype(str).str.zfill(2)
            + "hs - "
            + (df.index.hour + 1).astype(str).str.zfill(2)
            + "hs"
        )

        # O intervalo deve ser o index
        df = df.set_index("intervalo")

        # Adiciona o total de cada coluna
        totals = df.sum(axis=0).to_frame().T
        totals.index = ["Total"]
        df = pd.concat([df, totals])

        result_list = []

        # Para cada máquina no DataFrame
        for col in df.columns:
            if col.startswith("TMF"):  # se é uma coluna de máquina
                for idx, row in df.iterrows():
                    result_list.append(
                        {
                            "maquina_id": col,
                            "intervalo": idx,
                            "total": int(row[col]) if pd.notna(row[col]) else 0,
                        }
                    )

        # Converter para DataFrame e remover linhas com total zero se necessário
        result_df = pd.DataFrame(result_list)
        # result_df = result_df[result_df["total"] > 0].reset_index(drop=True)

        return result_df
