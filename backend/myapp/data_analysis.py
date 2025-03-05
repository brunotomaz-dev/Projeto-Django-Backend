"""Módulo com classes de análise de dados"""

from datetime import datetime

import numpy as np
import pandas as pd

from .utils import (
    AF_REP,
    CICLOS_BOLINHA,
    CICLOS_ESPERADOS,
    DESC_EFF,
    DESC_PERF,
    DESC_REP,
    NOT_EFF,
    NOT_PERF,
    IndicatorType,
)

pd.set_option("future.no_silent_downcasting", True)


class CleanData:
    """Helper class for data cleaning."""

    def __init__(self) -> None:
        pass

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa os dados básicos no DataFrame fornecido.

        Parâmetros:
        df (pd.DataFrame): O DataFrame contendo os dados a serem limpos.

        Retorna:
        pd.DataFrame: O DataFrame limpo.

        Etapas:
        1. Remove valores duplicados do DataFrame.
        2. Remove linhas com valores ausentes em colunas específicas.
        3. Remove milissegundos da coluna 'hora_registro'.
        4. Converte as colunas 'data_registro' e 'hora_registro' para os tipos de dados corretos.
        5. Substitui valores NaN na coluna 'linha' por 0 e converte para inteiro.
        6. Remove linhas onde 'linha' é 0.

        """

        # Remove valores duplicados, caso existam
        df = df.drop_duplicates()

        # Remove as linha com valores nulos que não podem faltar
        df = df.dropna(subset=["maquina_id", "data_registro", "hora_registro"])

        # Remover os milissegundos da coluna hora_registro
        df.hora_registro = df.hora_registro.astype(str).str.split(".").str[0]

        # Substitui os valores NaN por 0 e depois converte para inteiro
        if "linha" in df.columns:
            df.linha = df.linha.fillna(0).astype(int)
            # Remover onde a linha for 0
            df = df[df.linha != 0]

            df["fabrica"] = df.linha.apply(lambda x: 1 if x in range(1, 10) else 2)

        # Se existir a coluna operador_id, fazer alguns ajustes
        if "operador_id" in df.columns:
            df.operador_id = df.operador_id.fillna(0).astype(int)
            df.operador_id = df.operador_id.astype(str).str.zfill(6)
            df.operador_id = df.operador_id.replace("000000", None)
            df.os_numero = df.os_numero.replace("0", None)
            df = df.infer_objects(copy=False)

        return df


# ================================================================================================ #
#                                        UNIÃO DE INFO E IHM                                       #
# ================================================================================================ #
class InfoIHMJoin:
    """
    Essa classe é responsável por juntar os DataFrames de info e ihm.

    Parâmetros:
    df_info (pd.DataFrame): DataFrame de info.
    df_ihm (pd.DataFrame): DataFrame de ihm.
    """

    def __init__(self, df_ihm: pd.DataFrame, df_info: pd.DataFrame) -> None:
        self.df_ihm = df_ihm
        self.df_info = df_info
        self.clean_data = CleanData()

    @staticmethod
    def __line_adjust(df_ihm: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
        # Cria um dicionário maquina/linha
        maq_line_dict = dict(zip(df_ihm["maquina_id"], df_ihm["linha"]))
        maq_fab_dict = dict(zip(df_ihm["maquina_id"], df_ihm["fabrica"]))

        df["linha"] = df["linha"].fillna(df["maquina_id"].map(maq_line_dict))
        df["fabrica"] = df["fabrica"].fillna(df["maquina_id"].map(maq_fab_dict))

        return df

        # @staticmethod
        # def __line_adjust_date_opt(df_ihm: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
        #     ml_map = df_ihm[["data_registro", "maquina_id", "linha", "fabrica"]].drop_duplicates()

        #     df = pd.merge_asof(
        #         df,
        #         ml_map,
        #         on="data_registro",
        #         by="maquina_id",
        #         direction="nearest",
        #         suffixes=("", "_aux"),
        #     )

        # # Preencher linhas vazias
        # df["linha"] = df["linha"].fillna(df["linha_aux"])
        # df["fabrica"] = df["fabrica"].fillna(df["fabrica_aux"])

        # # Remover coluna auxiliar
        # df = df.drop(["linha_aux", "fabrica_aux"], axis=1)

        # return df

    def __clean_merge(self) -> pd.DataFrame:
        """Une os DataFrames de info e ihm."""

        df_ihm = self.df_ihm
        df_info = self.df_info

        df_ihm = self.clean_data.clean_data(df_ihm)
        df_info = self.clean_data.clean_data(df_info)

        # Ajustar os dados - Data de registro
        df_ihm.data_registro = pd.to_datetime(df_ihm.data_registro)
        df_info.data_registro = pd.to_datetime(df_info.data_registro)

        # Ajustar os dados - Hora de registro
        df_ihm.hora_registro = pd.to_datetime(df_ihm.hora_registro, format="%H:%M:%S").dt.time
        df_info.hora_registro = pd.to_datetime(df_info.hora_registro, format="%H:%M:%S").dt.time

        # Criar dados - Coluna de Data e Hora de registro
        df_ihm["data_hora"] = pd.to_datetime(
            df_ihm.data_registro.astype(str) + " " + df_ihm.hora_registro.astype(str)
        )
        df_info["data_hora"] = pd.to_datetime(
            df_info.data_registro.astype(str) + " " + df_info.hora_registro.astype(str)
        )

        # Classificar os dados - Data e Hora de registro
        df_ihm = df_ihm.sort_values(by="data_hora")
        df_info = df_info.sort_values(by="data_hora")

        # Juntar os DataFrames
        df = pd.merge_asof(
            df_info,
            df_ihm,
            on="data_hora",
            by="maquina_id",
            direction="nearest",
            tolerance=pd.Timedelta("3m30sec"),
        )

        # Ajuste de Linha que não leva em conta a data
        df = self.__line_adjust(df_ihm, df)

        # NOTE A ser usado em casos que precisa levar em conta a data
        # df = self.__line_adjust_date_opt(df_ihm, df)

        # Define o tipo para colunas de ciclos e produção
        df.contagem_total_ciclos = df.contagem_total_ciclos.astype("Int64")
        df.contagem_total_produzido = df.contagem_total_produzido.astype("Int64")

        # Ajustar Status - true/false para rodando/parada
        df.status = df.status.map(
            {"true": "rodando", "false": "parada"}
        )  # NOTE - .map funciona bem para Series do pandas

        # Reordenar as colunas
        df = df[
            [
                "fabrica",
                "linha",
                "maquina_id",
                "turno",
                "contagem_total_ciclos",
                "contagem_total_produzido",
                "data_registro_x",
                "hora_registro_x",
                "status",
                "data_registro_y",
                "hora_registro_y",
                "motivo",
                "equipamento",
                "problema",
                "causa",
                "os_numero",
                "operador_id",
                "s_backup",
            ]
        ]

        # Renomear as colunas
        df = df.rename(
            columns={
                "data_registro_x": "data_registro",
                "hora_registro_x": "hora_registro",
                "data_registro_y": "data_registro_ihm",
                "hora_registro_y": "hora_registro_ihm",
            }
        )

        # Reordenar
        df = df.sort_values(by=["linha", "data_registro", "hora_registro"])

        # Reiniciar o index
        df = df.reset_index(drop=True)

        return df

    @staticmethod
    def __identify_changes(df: pd.DataFrame, col: list[str]) -> pd.Series:
        return df[col].ne(df[col].shift())

    def __status_change(self, df: pd.DataFrame) -> pd.DataFrame:

        # Verificação de mudança - status, maquina_id e turno
        columns = ["status", "maquina_id", "turno"]
        for col in columns:
            df[f"{col}_change"] = self.__identify_changes(df, col)

        # Criar coluna de mudança
        df["change"] = df[[f"{col}_change" for col in columns]].any(axis=1)

        # Criar grupo de mudança
        df["group"] = df.change.cumsum()

        return df

    @staticmethod
    def __fill_occ(df: pd.DataFrame) -> pd.DataFrame:

        # Colunas de interesse
        fill_cols = [
            "motivo",
            "equipamento",
            "problema",
            "causa",
            "os_numero",
            "operador_id",
            "s_backup",
            "data_registro_ihm",
            "hora_registro_ihm",
        ]

        # Preencher os valores
        df[fill_cols] = df.groupby("group")[fill_cols].ffill()
        df[fill_cols] = df.groupby("group")[fill_cols].bfill()
        # NOTE - melhor performance do que código original

        # Se os dado de uma coluna for '' ou ' ', substituir por NaN
        df = df.replace(r"^s*$", None, regex=True)
        # O ^ indica o início de uma string, o $ indica o fim de uma string,
        # e s* zero ou mais espaços em branco

        # Ajuste de valores - caso maquina esteja rodando, não há motivo de parada
        mask = df.status == "rodando"
        df.loc[mask, fill_cols] = None

        return df

    @staticmethod
    def __motivo_change(df: pd.DataFrame) -> pd.DataFrame:

        # Identifica mudanças
        mask = (df.motivo.ne(df.motivo.shift()) & df.motivo.notnull()) | (
            df.causa.ne(df.causa.shift()) & df.causa.notnull()
        )

        # Cria a coluna motivo_change
        df["motivo_change"] = mask

        # Atualiza o change
        df.change = df.change | df.motivo_change

        # Atualiza o group
        df["group"] = df.change.cumsum()

        return df

    @staticmethod
    def __calculate_time_difference(df: pd.DataFrame) -> pd.DataFrame:

        # Cria coluna para data e hora de registro
        df["data_hora"] = pd.to_datetime(
            df.data_registro.astype(str) + " " + df.hora_registro.astype(str)
        )

        # Agrupa por grupo e calcula a diferença de tempo
        df = (
            df.groupby(["group"])
            .agg(
                fabrica=("fabrica", "first"),
                linha=("linha", "first"),
                maquina_id=("maquina_id", "first"),
                turno=("turno", "first"),
                status=("status", "first"),
                data_registro=("data_registro", "first"),
                hora_registro=("hora_registro", "first"),
                motivo=("motivo", "first"),
                equipamento=("equipamento", "first"),
                problema=("problema", "first"),
                causa=("causa", "first"),
                os_numero=("os_numero", "first"),
                operador_id=("operador_id", "first"),
                data_registro_ihm=("data_registro_ihm", "first"),
                hora_registro_ihm=("hora_registro_ihm", "first"),
                s_backup=("s_backup", "first"),
                data_hora=("data_hora", "first"),
                change=("change", "first"),
                maquina_id_change=("maquina_id_change", "first"),
                motivo_change=("motivo_change", "first"),
            )
            .reset_index()
        )

        # Coluna com a data e hora 'final'
        df["data_hora_final"] = df.data_hora.shift(-1).where((~df.maquina_id_change).shift(-1))

        # Se a data hora final for nula (último registro), preencher com a data e hora atual
        now = pd.to_datetime("now").floor("s")
        df.data_hora_final = df.data_hora_final.fillna(now)

        # Calcula a diferença de tempo entre data e hora final e inicial
        df["tempo"] = (
            ((df.data_hora_final - df.data_hora).dt.total_seconds() / 60).round().astype(int)
        )

        # Ajustar máximo e mínimo 0, 480
        df.tempo = df.tempo.clip(0, 480)

        # Ajustar caso tempo seja 478
        mask = df.tempo == 478
        df.loc[mask, "tempo"] = 480

        return df

    def join_data(self) -> pd.DataFrame:
        """Junta os DataFrames de info e ihm."""

        # Limpa e junta os DataFrames
        df = self.__clean_merge()

        # Identifica mudanças
        df = self.__status_change(df)

        # Preenche os valores
        df = self.__fill_occ(df)

        # Identifica mudanças de motivo
        df = self.__motivo_change(df)

        # Calcula a diferença de tempo
        df = self.__calculate_time_difference(df)

        # Remove colunas desnecessárias
        df = df.drop(
            columns=[
                "maquina_id_change",
                "change",
                "maquina_id_change",
                "motivo_change",
                "group",
            ]
        )

        # Ajuste Saída Backup
        mask = df.motivo == "Saída para Backup"

        # Ajuste das colunas de backup
        df.s_backup = np.where(mask, df.s_backup, None)
        df.problema = np.where(mask, "Parada Planejada", df.problema)
        df.causa = np.where(mask, "Backup", df.causa)

        # Ajustando a fabrica
        df.fabrica = df.fabrica.fillna(0).clip(lower=0).astype(int)
        df = df[df.fabrica.isin(range(1, 15))]

        # Ajusta a data de registro do IHM
        df.data_registro_ihm = df.data_registro_ihm.fillna(df.data_registro)
        df.hora_registro_ihm = df.hora_registro_ihm.fillna(df.hora_registro)

        return df


# ================================================================================================ #
#                                             PRODUÇÃO                                             #
# ================================================================================================ #
def clean_hora_registro(hora_str: str):
    """
    Remove os milissegundos da coluna 'hora_registro' e converte para o tipo correto.

    Parâmetros:
    df (pd.DataFrame): O DataFrame contendo os dados a serem ajustados.

    Retorna:
    pd.DataFrame: O DataFrame com a coluna 'hora_registro' ajustada.
    """
    try:
        # Remove milissegundos se existirem
        if "." in str(hora_str):
            hora_str = str(hora_str).split(".", maxsplit=1)[0]

        # Converte para datetime e extrai time
        return pd.to_datetime(hora_str, format="%H:%M:%S").time()
    except ValueError:
        return None


def join_qual_prod(prod: pd.DataFrame, qual: pd.DataFrame):
    """
    Junta os DataFrames de produção e qualidade.

    Parâmetros:
    prod (pd.DataFrame): O DataFrame de produção.
    qual (pd.DataFrame): O DataFrame de qualidade.

    Retorna:
    pd.DataFrame: O DataFrame com as informações de produção e qualidade unidas.

    Etapas:
    1. Remove valores duplicados, caso existam.
    2. Remove as linha com valores nulos que não podem faltar.
    3. Ajusta as colunas de data para o tipo correto.
    4. Define os turnos.
    5. Agrupa os dados por linha, máquina, data e turno.
    6. Realiza o merge dos dataframes.
    7. Preenche os valores nulos.
    8. Calcula a produção - ajuste para possível erro no sensor (faixa de 5%).
    9. Ordena os valores.
    10. Ajustar para inteiros.

    """
    qual = qual.copy()
    prod = prod.copy()

    # Ajusta as colunas de data
    qual.data_registro = pd.to_datetime(qual.data_registro)
    prod.data_registro = pd.to_datetime(prod.data_registro)
    qual["hora_registro"] = qual["hora_registro"].apply(clean_hora_registro)

    # Definir os turnos
    qual["turno"] = qual["hora_registro"].apply(lambda x: x.hour) // 8
    qual["turno"] = qual["turno"].map({0: "NOT", 1: "MAT", 2: "VES"})

    qual = qual.drop(columns=["hora_registro", "recno"])

    # Agrupar os dados
    qual = (
        qual.groupby(["linha", "maquina_id", "data_registro", "turno"]).sum().round(3).reset_index()
    )

    # Classifica os dataframes por data
    qual = qual.sort_values(by="data_registro")
    prod = prod.sort_values(by="data_registro")

    # Realiza o merge dos dataframes
    df = pd.merge(
        prod,
        qual,
        on=["linha", "maquina_id", "data_registro", "turno"],
        how="left",
    )

    # Preenche os valores nulos
    df = df.fillna(0)

    # Calcula a produção - ajuste para possível erro no sensor (faixa de 5%)
    mask = (df.total_ciclos - df.total_produzido_sensor) / df.total_ciclos < 0.05
    ciclos = df.total_ciclos - df.bdj_vazias - df.bdj_retrabalho
    sensor = df.total_produzido_sensor - df.bdj_retrabalho
    df["total_produzido"] = np.where(mask, sensor, ciclos)

    # Ordena os valores
    df = df.sort_values(by=["data_registro", "linha", "turno"])

    # Ajustar para inteiros
    df.total_produzido = df.total_produzido.astype(int)
    df.total_produzido_sensor = df.total_produzido_sensor.astype(int)
    df.bdj_vazias = df.bdj_vazias.astype(int)
    df.bdj_retrabalho = df.bdj_retrabalho.astype(int)

    return df


# ================================================================================================ #
#                                      INDICADORES DE PRODUÇÃO                                     #
# ================================================================================================ #


class ProductionIndicators:
    """Classe responsável por gerar indicadores de produção"""

    def __init__(self) -> None:
        pass

    @staticmethod
    def __calculate_discount_time(
        df: pd.DataFrame,
        desc_dict: dict[str, int],
        skip_list: list[str],
        indicator: IndicatorType,
    ) -> pd.DataFrame:
        """Calcula o tempo de desconto"""
        df = df.copy()

        # Cria coluna de desconto
        df["desconto"] = 0

        # Lidar com situações que não afetam o indicador
        mask = df[["motivo", "problema", "causa"]].apply(lambda x: x.isin(skip_list).any(), axis=1)
        df.loc[mask, "desconto"] = 0 if indicator == IndicatorType.REPAIR else df["tempo"]

        # Cria um dict para indicadores
        indicator_dict = {
            IndicatorType.EFFICIENCY: df,
            IndicatorType.PERFORMANCE: df[~mask],
            IndicatorType.REPAIR: df[mask],
        }

        df = indicator_dict[indicator].reset_index(drop=True)

        # Aplica o desconto de acordo com as colunas "motivo" ou "problema" ou "causa"
        for key, value in desc_dict.items():
            mask = (
                df[["motivo", "problema", "causa"]]
                .apply(lambda x, key=key: x.str.contains(key, case=False, na=False))
                .any(axis=1)
            )
            df.loc[mask, "desconto"] = value

        # Caso o desconto seja maior que o tempo, o desconto deve ser igual ao tempo
        df.loc[:, "desconto"] = df[["desconto", "tempo"]].min(axis=1)

        # Calcula o excedente, sendo o valor mínimo 0
        df.loc[:, "excedente"] = (df.tempo - df.desconto).clip(lower=0)

        return df

    @staticmethod
    def __get_elapsed_time(turno: str) -> int:
        """
        Calcula o tempo decorrido.

        """
        # Agora
        now = datetime.now()

        if turno == "MAT" and 8 <= now.hour < 16:
            elapsed_time = now - datetime(now.year, now.month, now.day, 8, 0, 0)
        elif turno == "VES" and 16 <= now.hour < 24:
            elapsed_time = now - datetime(now.year, now.month, now.day, 16, 0, 0)
        elif turno == "NOT" and 0 <= now.hour < 8:
            elapsed_time = now - datetime(now.year, now.month, now.day, 0, 0, 0)
        else:
            return 480

        return elapsed_time.total_seconds() / 60

    def __get_expected_production_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula o tempo esperado de produção.
        """
        df["tempo_esperado"] = df.apply(
            lambda row: max(
                1,
                (
                    np.floor(self.__get_elapsed_time(row.turno) - row.desconto)
                    if row.data_registro.date() == pd.to_datetime("today").date()
                    else 480 - row.desconto
                ),
            ),
            axis=1,
        )

        return df

    def create_indicators(
        self, info: pd.DataFrame, prod: pd.DataFrame, indicator: IndicatorType
    ) -> pd.DataFrame:
        """Cria indicadores de produtividade"""

        df_info = info.copy()
        df_prod = prod.copy()

        # Separa onde está parada
        df_stops = df_info[df_info.status == "parada"]
        # Reinicia o index
        df_stops = df_stops.reset_index(drop=True)

        # Dict com os descontos
        desc_dict = {
            IndicatorType.EFFICIENCY: DESC_EFF,
            IndicatorType.PERFORMANCE: DESC_PERF,
            IndicatorType.REPAIR: DESC_REP,
        }[indicator]

        skip_dict = {
            IndicatorType.EFFICIENCY: NOT_EFF,
            IndicatorType.PERFORMANCE: NOT_PERF,
            IndicatorType.REPAIR: AF_REP,
        }[indicator]

        # Ajuste de parada programada para perf e reparos para ser np.nan - Feito nos ajustes
        paradas_programadas = pd.Series()
        if indicator != IndicatorType.EFFICIENCY:
            mask = (df_stops.causa.isin(["Sem Produção", "Backup"])) & (df_stops.tempo >= 478)
            paradas_programadas = df_stops[mask][["data_registro", "turno", "linha"]]

        # ================================== Calcula O Indicador ================================= #
        # Calcula o tempo de desconto
        df_stops = self.__calculate_discount_time(df_stops, desc_dict, skip_dict, indicator)

        # Agrupa para ter o valor total de tempo e de desconto
        df_stops = (
            df_stops.groupby(["maquina_id", "linha", "data_registro", "turno"], observed=False)
            .agg(
                tempo=("tempo", "sum"),
                desconto=("desconto", "sum"),
                excedente=("excedente", "sum"),
            )
            .reset_index()
        )

        # Ajusta a data por garantia
        df_stops.data_registro = pd.to_datetime(df_stops.data_registro)
        df_prod.data_registro = pd.to_datetime(df_prod.data_registro)

        # Une os dois dataframes
        df = pd.merge(
            df_prod,
            df_stops,
            how="left",
            on=["maquina_id", "linha", "data_registro", "turno"],
        )

        # Preenche os valores nulos
        df = df.fillna(0)

        # Nova coluna para o tempo esperado de produção
        df = self.__get_expected_production_time(df)

        # Dict de funções para ajustes dos indicadores
        indicator_adjustment_functions = {
            IndicatorType.EFFICIENCY: self.__eff_adjust,
            IndicatorType.PERFORMANCE: self.__adjust,
            IndicatorType.REPAIR: self.__adjust,
        }[indicator]

        # Ajusta o indicador
        if indicator != IndicatorType.EFFICIENCY:
            df: pd.DataFrame = indicator_adjustment_functions(df, indicator, paradas_programadas)
        else:
            df: pd.DataFrame = indicator_adjustment_functions(df, indicator)

        df["fabrica"] = df.linha.apply(lambda x: 1 if x in range(1, 10) else 2)

        # Transformar algumas colunas em inteiro
        df.tempo = df.tempo.astype(int)
        df.desconto = df.desconto.astype(int)
        df.excedente = df.excedente.astype(int)
        df.tempo_esperado = df.tempo_esperado.astype(int)
        df.total_produzido = df.total_produzido.astype(int)
        if indicator == IndicatorType.EFFICIENCY:
            df.producao_esperada = df.producao_esperada.astype(int)

        # Ajustar a ordem das colunas
        cols_eff = [
            "fabrica",
            "linha",
            "maquina_id",
            "turno",
            "data_registro",
            "tempo",
            "desconto",
            "excedente",
            "tempo_esperado",
            "total_produzido",
            "producao_esperada",  # Cspell: words producao
            indicator.value,
        ]

        cols = [
            "fabrica",
            "linha",
            "maquina_id",
            "turno",
            "data_registro",
            "tempo",
            "desconto",
            "excedente",
            "tempo_esperado",
            indicator.value,
        ]

        return df[cols] if indicator != IndicatorType.EFFICIENCY else df[cols_eff]

    @staticmethod
    def __eff_adjust(df: pd.DataFrame, indicator: IndicatorType) -> pd.DataFrame:
        """
        Ajusta o indicador de eficiência.
        """

        # Variável para identificar quando o produto possui a palavra " BOL "
        mask_bolinha = df["produto"].str.contains("BOL ")

        # Nova coluna para o tempo esperado de produção
        df["producao_esperada"] = round(
            df["tempo_esperado"] * (CICLOS_BOLINHA * 2) * mask_bolinha
            + df["tempo_esperado"] * (CICLOS_ESPERADOS * 2) * ~mask_bolinha,
            0,
        )

        # Coluna de eficiência
        df[indicator.value] = (df.total_produzido / df.producao_esperada).round(3)

        # Corrige os valores nulos ou incorretos
        df[indicator.value] = df[indicator.value].replace([np.inf, -np.inf], 0).fillna(0)

        # Ajustar a eficiência para np.nan se produção esperada for 0
        mask = (df.producao_esperada == 0) & (df[indicator.value] == 0)
        df.loc[mask, indicator.value] = 0

        # Corrigir a eficiência para 0 caso seja negativa
        df[indicator.value] = np.where(df[indicator.value] < 0, 0, df[indicator.value])

        # Ajustar eficiência para tempo de produção esperado menor que 10
        mask = df.tempo_esperado < 10
        df.loc[mask, indicator.value] = 0
        df.loc[mask, "producao_esperada"] = 0
        df.loc[mask, "tempo_esperado"] = 0

        # Ajustar o indicador caso o valor seja maior que 120 e o tempo esperado menor que 15 min
        mask = (df[indicator.value] > 1.2) & (df.tempo_esperado < 15)
        df.loc[mask, indicator.value] = 1.2

        # Definir valor máximo e mínimo do indicador
        df[indicator.value] = df[indicator.value].clip(0, 1.5)

        return df

    @staticmethod
    def __adjust(
        df: pd.DataFrame, indicador: IndicatorType, paradas_programadas: pd.Series
    ) -> pd.DataFrame:
        """
        Ajusta os indicadores de performance e reparos.
        """

        # Coluna do indicador
        df[indicador.value] = (df.excedente / df.tempo_esperado).round(3)

        # Corrige os valores nulos ou incorretos
        df[indicador.value] = df[indicador.value].replace([np.inf, -np.inf], 0).fillna(0)

        # Ajuste para paradas programadas
        paradas_programadas["programada"] = 1

        # Garantir que data_registro seja datetime
        paradas_programadas.data_registro = pd.to_datetime(paradas_programadas.data_registro)
        df.data_registro = pd.to_datetime(df.data_registro)

        # Une os dois dataframes
        df = pd.merge(df, paradas_programadas, how="left", on=["data_registro", "turno", "linha"])

        # np.nan para paradas programadas
        mask = df.programada == 1
        df.loc[mask, indicador.value] = 0
        df.loc[mask, "tempo_esperado"] = 0

        # Remove a coluna programada
        df = df.drop(columns="programada")

        # Definir valor máximo e mínimo do indicador
        df[indicador.value] = df[indicador.value].clip(0, 1)

        return df
