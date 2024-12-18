"""Módulo com classes de análise de dados"""

from datetime import datetime

import numpy as np
import pandas as pd
from django.utils.timezone import make_aware

from .utils import (
    AF_REP,
    CICLOS_BOLINHA,
    CICLOS_ESPERADOS,
    DESC_EFF,
    DESC_PERF,
    DESC_REP,
    NOT_EFF,
    NOT_PERF,
    TEMPO_AJUSTE,
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
    #     maq_line_map = df_ihm[
    #         ["data_registro", "maquina_id", "linha", "fabrica"]
    #         ].drop_duplicates()

    #     df = pd.merge_asof(
    #         df,
    #         maq_line_map,
    #         on="data_registro",
    #         by="maquina_id",
    #         direction="nearest",
    #         suffixes=("", "_aux"),
    #     )

    #     # Preencher linhas vazias
    #     df["linha"] = df["linha"].fillna(df["linha_aux"])
    #     df["fabrica"] = df["fabrica"].fillna(df["fabrica_aux"])

    #     # Remover coluna auxiliar
    #     df = df.drop(["linha_aux", "fabrica_aux"], axis=1)

    #     return df

    def join_data(self) -> pd.DataFrame:
        """Une os DataFrames de info e ihm."""

        df_ihm = self.df_ihm
        df_info = self.df_info

        df_ihm = self.clean_data.clean_data(df_ihm)
        df_info = self.clean_data.clean_data(df_info)

        df_ihm.data_registro = pd.to_datetime(df_ihm.data_registro)
        df_info.data_registro = pd.to_datetime(df_info.data_registro)
        df_ihm.hora_registro = pd.to_datetime(df_ihm.hora_registro, format="%H:%M:%S").dt.time
        df_info.hora_registro = pd.to_datetime(df_info.hora_registro, format="%H:%M:%S").dt.time

        # Cria coluna com união de data e hora para df_ihm
        df_ihm["data_hora"] = pd.to_datetime(
            df_ihm.data_registro.astype(str) + " " + df_ihm.hora_registro.astype(str)
        )

        # Cria coluna com união de data e hora para df_info
        df_info["data_hora"] = pd.to_datetime(
            df_info.data_registro.astype(str) + " " + df_info.hora_registro.astype(str)
        )

        # Classifica os dataframes por data_hora
        df_ihm = df_ihm.sort_values(by="data_hora")
        df_info = df_info.sort_values(by="data_hora")

        # Realiza o merge dos dataframes
        df = pd.merge_asof(
            df_info,
            df_ihm,
            on="data_hora",
            by="maquina_id",
            direction="nearest",
            tolerance=pd.Timedelta("2 min"),
        )

        # Ajuste de Linha que não leva em conta a data
        df = self.__line_adjust(df_ihm, df)

        # NOTE A ser usado em casos que precisa levar em conta a data
        # df = self.__line_adjust_date_opt(df_ihm, df)

        # Define o tipo para colunas de ciclos e produção
        df.contagem_total_ciclos = df.contagem_total_ciclos.astype("Int64")
        df.contagem_total_produzido = df.contagem_total_produzido.astype("Int64")

        # Reordenar as colunas, mantendo só as necessárias
        df = df[
            [
                "fabrica",
                "linha",
                "maquina_id",
                "turno",
                "status",
                "contagem_total_ciclos",
                "contagem_total_produzido",
                "data_registro_x",
                "hora_registro_x",
                "motivo",
                "equipamento",
                "problema",
                "causa",
                "os_numero",
                "operador_id",
                "data_registro_y",
                "hora_registro_y",
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

        # Reordenar o dataframe
        df = df.sort_values(by=["linha", "data_registro", "hora_registro"])

        # Reiniciar o index
        df = df.reset_index(drop=True)

        # ========= Ajustes Na Posição Das Paradas E Soma Dos Tempos De Parada E Rodando ========= #
        df = self.get_info_ihm_adjusted(df)

        # Ajustando a fabrica
        df.fabrica = df.fabrica.fillna(0).clip(lower=0).astype(int)
        df = df[df.fabrica.isin(range(1, 15))]

        # Se a data hora final for menor que a data hora, ajustar para data hora final para agora
        now = pd.Timestamp.now()
        df.data_hora_final = np.where(
            df.data_hora_final < df.data_hora, now.round("s"), df.data_hora_final
        )
        df.data_hora_final = pd.to_datetime(df.data_hora_final)

        # ========= Ajusta A Data Hora E Data Hora Final Para O Timezone Correto Para Salvar No Db #
        df.data_hora = df.data_hora.apply(make_aware)
        df.data_hora_final = df.data_hora_final.apply(make_aware)

        df.data_registro_ihm = df.data_registro_ihm.fillna(df.data_registro)
        df.hora_registro_ihm = df.hora_registro_ihm.fillna(df.hora_registro)

        return df

    # ================================== Métodos Complementares ================================== #

    @staticmethod
    def __identify_changes(df: pd.DataFrame, column: str) -> pd.Series:

        # Identifica mudanças em uma coluna
        result = df[column].ne(df[column].shift())

        return result

    def __status_change(self, df: pd.DataFrame) -> pd.DataFrame:

        # Checa se houve mudança de status, maquina_id e turno
        columns_to_check = ["status", "maquina_id", "turno"]
        for column in columns_to_check:
            df[f"{column}_change"] = self.__identify_changes(df, column)

        # Coluna auxiliar que identifica se houve alguma mudança
        df["change"] = df[["status_change", "maquina_id_change", "turno_change"]].any(axis=1)

        return df

    @staticmethod
    def __fill_occ(df: pd.DataFrame) -> pd.DataFrame:

        # Preenche os valores nulos de paradas
        fill_columns = [
            "motivo",
            "equipamento",
            "problema",
            "causa",
            "os_numero",
            "operador_id",
            "data_registro_ihm",
            "hora_registro_ihm",
            "s_backup",
        ]
        for col in fill_columns:
            df[col] = df.groupby("group")[col].transform(lambda x: x.ffill().bfill())
            df = df.infer_objects(copy=False)

        # Se os dado de uma coluna for '' ou ' ', substituir por NaN
        df = df.replace(r"^s*$", None, regex=True)
        # O ^ indica o início de uma string, o $ indica o fim de uma string,
        # e s* zero ou mais espaços em branco

        return df

    @staticmethod
    def __group_and_calc_time(df: pd.DataFrame) -> pd.DataFrame:

        # Agrupa as mudanças
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
                change_date=("change_date", "first"),
                motivo_change=("motivo_change", "first"),
            )
            .reset_index()
        )

        # Nova coluna com a data_hora_final do status/parada
        df["data_hora_final"] = (
            df.groupby("maquina_id")["data_hora"].shift(-1).where(~df["maquina_id_change"])
        )

        # Atualiza a hora final caso mude a máquina
        mask = df["maquina_id_change"]
        df.data_hora_final = np.where(
            mask,
            df.change_date.shift(-1),
            df.data_hora_final,
        )

        # =============== Atualização Para Hora Final No Caso De Mudança De Turno =============== #
        # Dicionário com o horário de término de cada turno
        turno_end_time = {
            "NOT": pd.to_timedelta("08:01:00"),
            "MAT": pd.to_timedelta("16:01:00"),
            "VES": pd.to_timedelta("00:01:00"),
        }

        # Nova coluna com o horário de término do turno
        df["turno_end_time"] = df.turno.map(turno_end_time)

        # Determina a data e hora atual
        now = pd.Timestamp.now()

        # Nova coluna para indicar se a data é a mesma do dia atual
        df["is_today"] = pd.to_datetime(df.data_hora).dt.date == now.date()

        # Determina o turno atual com base na hora atual
        if now.hour in range(0, 8):
            current_shift = "NOT"
        elif now.hour in range(8, 16):
            current_shift = "MAT"
        else:
            current_shift = "VES"

        # Atualiza a hora final caso haja mudança de turno e o turno não seja o turno atual
        mask = (df.turno != df.turno.shift(-1)) & ~((df["is_today"]) & (df.turno == current_shift))

        df.data_hora_final = np.where(
            mask & (df.turno == "VES"),
            (df.data_hora.dt.normalize() + pd.DateOffset(days=1)) + df.turno_end_time,
            np.where(
                mask,
                df.data_hora.dt.normalize() + df.turno_end_time,
                df.data_hora_final,
            ),
        )

        # Se a data_hora_final for nula, atualiza com a data_hora + 1 minuto
        df.data_hora_final = df.data_hora_final.fillna(df.data_hora + pd.Timedelta("1m"))

        # Remove coluna auxiliar
        df = df.drop(columns=["turno_end_time", "is_today"])

        # Caso a data_hora_final seja nula, remove a linha
        df = df.dropna(subset=["data_hora_final"]).reset_index(drop=True)

        # Calcula o tempo de cada status
        df["tempo"] = (
            pd.to_datetime(df.data_hora_final) - pd.to_datetime(df.data_hora)
        ).dt.total_seconds() / 60

        # Arredondar e converter para inteiro
        df.tempo = df.tempo.round().astype(int)

        # Se o tempo for maior que 478, e o motivo for parada programada ou limpeza ajustar para 480
        mask = (df.tempo > 478) & (df.motivo.isin(["Parada Programada", "Limpeza"]))
        df.tempo = np.where(mask, 480, df.tempo)

        return df

    def get_info_ihm_adjusted(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieves adjusted information from the IHM (Interface Homem-Máquina).

        This method performs data reading, cleaning, and joining operations to retrieve adjusted
        information from the IHM.
        It identifies status changes, fills null values for stops, calculates stop or running times,
        and performs other adjustments.

        Returns:
            pd.DataFrame: The adjusted information from the IHM.
        """

        # Realiza a leitura, limpeza e junção dos dados
        df_joined = dataframe

        # Ajusta a nomenclatura de status
        df_joined.status = np.where(df_joined.status == "true", "rodando", "parada")

        # ========================= Identifica Onde Há Mudança De Status ========================= #

        df_joined = self.__status_change(df_joined)

        # ========================= Preenchendo Valores Nulos De Paradas ========================= #

        # Cria um grupo para cada mudança de status
        df_joined["group"] = df_joined.change.cumsum()

        # Preenche os valores nulos de paradas
        df_joined = self.__fill_occ(df_joined)

        # Coluna auxiliar para identificar mudança na coluna motivo, se não for nula
        mask = (df_joined.motivo.ne(df_joined.motivo.shift())) | (
            df_joined.causa.ne(df_joined.causa.shift())
        )
        df_joined["motivo_change"] = mask & df_joined.motivo.notnull()

        # Atualiza o change
        df_joined.change = df_joined.change | df_joined.motivo_change

        # Refaz o grupo para considerar a mudança na coluna motivo
        df_joined.group = df_joined.change.cumsum()

        # Coluna auxiliar para unir data e hora
        df_joined["data_hora"] = pd.to_datetime(
            df_joined.data_registro.astype(str) + " " + df_joined.hora_registro.astype(str)
        )

        # Coluna auxiliar para identificar a data/hora da mudança
        df_joined["change_date"] = (
            df_joined.groupby("maquina_id")["data_hora"].shift(0).where(df_joined.change)
        )

        # =========================== Calcula O Tempo Parada Ou Rodando ========================== #

        # Calcula os tempos
        df_joined = self.__group_and_calc_time(df_joined)

        # Ajustar status para levar em conta testes
        mask = (
            (df_joined.status == "rodando")
            & (df_joined.tempo <= TEMPO_AJUSTE)
            & (df_joined.turno.eq(df_joined.turno.shift(-1)))
            & (df_joined.motivo.shift() != "Parada Programada")
        )

        df_joined.status = np.where(mask, "parada", df_joined.status)

        # Ajustando o motivo change
        df_joined.motivo_change = np.where(mask, False, df_joined.motivo_change.shift(-1))

        mask = (
            (df_joined.status == "rodando")
            & (df_joined.tempo <= TEMPO_AJUSTE)
            & (df_joined.turno.eq(df_joined.turno.shift(-1)))
        )

        df_joined.status = np.where(mask, "parada", df_joined.status)

        # Ajuste em change para refletir alterações
        df_joined = self.__status_change(df_joined)
        df_joined.change = df_joined.change | df_joined.motivo_change

        # Refaz o group
        df_joined.group = df_joined.change.cumsum()

        # Preenche os valores nulos de paradas
        df_joined = self.__fill_occ(df_joined)

        # Recalcula os tempos
        df_joined = self.__group_and_calc_time(df_joined)

        # Se o tempo for negativo, ajustar para 0
        df_joined.tempo = df_joined.tempo.clip(lower=0)

        # Se o tempo for maior que 480 minutos, ajustar para 480
        df_joined.tempo = df_joined.tempo.clip(upper=480)

        # Se o tempo for maior que 478, ajustar para 480
        mask = df_joined.tempo > 478
        df_joined.tempo = np.where(mask, 480, df_joined.tempo)

        # Se o motivo não for saída para backup, ajustar s_backup para null
        mask = df_joined.motivo != "Saída para Backup"
        df_joined.s_backup = np.where(mask, None, df_joined.s_backup)

        # Remove colunas auxiliares
        df_joined = df_joined.drop(
            columns=[
                "maquina_id_change",
                "change",
                "maquina_id_change",
                "change_date",
                "motivo_change",
                "group",
            ]
        )

        # Remove dados de parada caso a máquina esteja rodando
        columns_to_adj = [
            "motivo",
            "equipamento",
            "problema",
            "causa",
            "operador_id",
        ]
        df_joined.loc[df_joined.status == "rodando", columns_to_adj] = None

        return df_joined


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
        df[indicator.value] = df[indicator.value].replace([np.inf, -np.inf], np.nan).fillna(0)

        # Ajustar a eficiência para np.nan se produção esperada for 0
        mask = (df.producao_esperada == 0) & (df[indicator.value] == 0)
        df.loc[mask, indicator.value] = np.nan

        # Corrigir a eficiência para 0 caso seja negativa
        df[indicator.value] = np.where(df[indicator.value] < 0, 0, df[indicator.value])

        # Ajustar eficiência para tempo de produção esperado menor que 10
        mask = df.tempo_esperado < 10
        df.loc[mask, indicator.value] = np.nan
        df.loc[mask, "producao_esperada"] = 0
        df.loc[mask, "tempo_esperado"] = 0

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
        df[indicador.value] = df[indicador.value].replace([np.inf, -np.inf], np.nan).fillna(0)

        # Ajuste para paradas programadas
        paradas_programadas["programada"] = 1

        # Garantir que data_registro seja datetime
        paradas_programadas.data_registro = pd.to_datetime(paradas_programadas.data_registro)
        df.data_registro = pd.to_datetime(df.data_registro)

        # Une os dois dataframes
        df = pd.merge(df, paradas_programadas, how="left", on=["data_registro", "turno", "linha"])

        # np.nan para paradas programadas
        mask = df.programada == 1
        df.loc[mask, indicador.value] = np.nan
        df.loc[mask, "tempo_esperado"] = 0

        # Remove a coluna programada
        df = df.drop(columns="programada")

        return df
