"""Módulo de visualizações do Django Rest Framework"""

import numpy as np
import pandas as pd
from django.db import connections

# from django.shortcuts import render
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import generics, status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.views import TokenObtainPairView

from .data_analysis import CleanData
from .filters import (
    InfoIHMFilter,
    MaquinaIHMFilter,
    MaquinaInfoFilter,
    QualidadeIHMFilter,
    QualProdFilter,
)
from .models import InfoIHM, MaquinaIHM, MaquinaInfo, QualidadeIHM, QualProd
from .serializers import (
    CustomTokenObtainPairSerializer,
    InfoIHMSerializer,
    MaquinaIHMSerializer,
    MaquinaInfoSerializer,
    QualidadeIHMSerializer,
    QualProdSerializer,
    RegisterSerializer,
)
from .utils import PESO_BANDEJAS, PESO_SACO


class CustomTokenObtainPairView(TokenObtainPairView):
    """Serializador de token"""

    serializer_class = CustomTokenObtainPairSerializer


class RegisterView(generics.CreateAPIView):
    """
    Registra um novo usuário no sistema.
    """

    serializer_class = RegisterSerializer


# Create your views here.
class MaquinaInfoViewSet(viewsets.ModelViewSet):
    """
    Exibe e edita informações de máquinas.

    Exemplo de uso:
    - GET /maquinainfo/?data_registro=2021-01-01
    - GET /maquinainfo/?data_registro__gt=2021-01-01
    - GET /maquinainfo/?data_registro__lt=2021-01-01
    - GET /maquinainfo/?data_registro__gt=2021-01-01&data_registro__lt=2021-01-31
    """

    # pylint: disable=E1101
    queryset = MaquinaInfo.objects.all()
    serializer_class = MaquinaInfoSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = MaquinaInfoFilter
    permission_classes = [IsAuthenticated]
    authentication_classes = [JWTAuthentication]


class MaquinaIHMViewSet(viewsets.ModelViewSet):
    """
    Exibe e edita informações de IHM de máquinas.

    Exemplo de uso:
    - GET /maquinaihm/?data_registro=2021-01-01
    - GET /maquinaihm/?data_registro__gt=2021-01-01
    - GET /maquinaihm/?data_registro__lt=2021-01-01
    - GET /maquinaihm/?data_registro__gt=2021-01-01&data_registro__lt=2021-01-31
    """

    # pylint: disable=E1101
    queryset = MaquinaIHM.objects.all()
    serializer_class = MaquinaIHMSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = MaquinaIHMFilter
    permission_classes = [IsAuthenticated]
    authentication_classes = [JWTAuthentication]

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        # Converte o queryset em um DataFrame
        df = pd.DataFrame(list(queryset.values()))

        # Realiza a limpeza de dados
        cleaner = CleanData()
        df_cleaned = cleaner.clean_data(df)

        # Cria coluna s_backup
        df_cleaned["s_backup"] = np.where(
            df_cleaned["equipamento"].astype(str).str.isdigit(), df_cleaned["equipamento"], None
        )

        # Remove os valores numéricos da coluna equipamento
        df_cleaned["equipamento"] = np.where(
            df_cleaned["equipamento"].astype(str).str.isdigit(),
            None,
            df_cleaned["equipamento"],
        )

        # Converte o DataFrame em uma um queryset
        cleaned_data = df_cleaned.to_dict("records")

        # Serializa o queryset limpo e retorna a resposta
        serializer = self.get_serializer(cleaned_data, many=True)
        return Response(serializer.data)


class InfoIHMViewSet(viewsets.ModelViewSet):
    """
    Exibe e edita informações de IHM de máquinas.

    Exemplo de uso:
    - GET /infoihm/?data_registro=2021-01-01
    - GET /infoihm/?data_registro__gt=2021-01-01
    - GET /infoihm/?data_registro__lt=2021-01-01
    - GET /infoihm/?data_registro__gt=2021-01-01&data_registro__lt=2021-01-31
    """

    # pylint: disable=E1101
    queryset = InfoIHM.objects.all()
    serializer_class = InfoIHMSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = InfoIHMFilter
    permission_classes = [IsAuthenticated]
    authentication_classes = [JWTAuthentication]


class QualidadeIHMViewSet(viewsets.ModelViewSet):
    """
    Exibe e edita informações de qualidade de IHM de máquinas.

    Exemplo de uso:
    - GET /qualidadeihm/?data_registro=2021-01-01
    - GET /qualidadeihm/?data_registro__gt=2021-01-01
    - GET /qualidadeihm/?data_registro__lt=2021-01-01
    - GET /qualidadeihm/?data_registro__gt=2021-01-01&data_registro__lt=2021-01-31
    """

    # pylint: disable=E1101
    queryset = QualidadeIHM.objects.all()
    serializer_class = QualidadeIHMSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = QualidadeIHMFilter
    permission_classes = [IsAuthenticated]
    authentication_classes = [JWTAuthentication]

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        # Converte o queryset em um DataFrame
        df = pd.DataFrame(list(queryset.values()))

        # Arredondar os valores
        df["bdj_vazias"] = df["bdj_vazias"].round(3)
        df["bdj_retrabalho"] = df["bdj_retrabalho"].round(3)
        df["descarte_paes"] = df["descarte_paes"].round(3)  # cSpell: disable-line
        df["descarte_paes_pasta"] = df["descarte_paes_pasta"].round(3)  # cSpell: disable-line
        df["descarte_pasta"] = df["descarte_pasta"].round(3)

        # Se houver descarte, calcular o valor
        df.loc[df["bdj_vazias"] > 0, "bdj_vazias"] = (
            (df["bdj_vazias"] - PESO_SACO) / PESO_BANDEJAS
        ).round(0)
        df.loc[df["bdj_retrabalho"] > 0, "bdj_retrabalho"] = (
            (df["bdj_retrabalho"] - PESO_SACO) / PESO_BANDEJAS
        ).round(0)

        # Ajustar para inteiro
        df["bdj_vazias"] = df["bdj_vazias"].astype(int)
        df["bdj_retrabalho"] = df["bdj_retrabalho"].astype(int)

        # Seta 0 como valor mínimo
        df["bdj_vazias"] = df["bdj_vazias"].clip(lower=0)
        df["bdj_retrabalho"] = df["bdj_retrabalho"].clip(lower=0)

        # Converte o DataFrame em uma um queryset
        df = df.to_dict("records")

        # Serializa o queryset limpo e retorna a resposta
        serializer = self.get_serializer(df, many=True)
        return Response(serializer.data)


class QualProdViewSet(viewsets.ModelViewSet):

    # pylint: disable=E1101
    queryset = QualProd.objects.all()
    serializer_class = QualProdSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = QualProdFilter
    permission_classes = [IsAuthenticated]
    authentication_classes = [JWTAuthentication]


class MaquinaInfoProductionViewSet(APIView):
    """
    Exibe informações de máquinas filtradas por período de tempo.

    Exemplo de uso:
    - GET /maquinainfo/period/?period=2021-01-01,2021-01-31
    """

    def get(self, request):
        """
        Retorna uma lista de dados de máquina filtrada por período de tempo.

        Query parameters:
        - period: período de tempo no formato 'YYYY-MM-DD,YYYY-MM-DD' (obrigatório)

        Resposta:
        - results: lista de dicionários com os dados da máquina
        """
        period = request.query_params.get("period", None)
        if not period:
            return Response(
                {"error": "Period parameter is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        first_day, last_day = self.parse_period(period)
        if not first_day or not last_day:
            return Response(
                {"error": "Invalid period format. Use 'YYYY-MM-DD,YYYY-MM-DD'"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = self.build_query(first_day, last_day)

        data = self.execute_query(query)

        return Response(data)

    def parse_period(self, period):
        """
        Analisa o período informado e retorna as datas de início e fim do período.

        Parâmetros:
        - period: período a ser analisado no formato 'YYYY-MM-DD,YYYY-MM-DD'

        Retorna:
        - first_day: data de início do período no formato 'YYYY-MM-DD'
        - last_day: data de fim do período no formato 'YYYY-MM-DD'

        Lança ValueError caso o período seja inválido.
        """
        try:
            first_day, last_day = period.split(",")
            first_day = pd.to_datetime(first_day).strftime("%Y-%m-%d")
            last_day = pd.to_datetime(last_day).strftime("%Y-%m-%d")
            return first_day, last_day
        except ValueError:
            return None, None

    def build_query(self, first_day, last_day):
        """
        Constrói as consultas para a API de informações de máquinas filtrada por período de tempo.

        Parâmetros:
        - first_day: data de início do período no formato 'YYYY-MM-DD'
        - last_day: data de fim do período no formato 'YYYY-MM-DD'

        Retorna:
        - query: consulta SQL para obter a lista de dados da máquina
        """
        query = f"""
            SELECT
                linha,
                maquina_id,
                turno,
                contagem_total_ciclos as total_ciclos,
                contagem_total_produzido as total_produzido_sensor,
                B1_DESC as produto,
                data_registro
            FROM (
                SELECT
                    (SELECT TOP 1 t2.fabrica FROM AUTOMACAO.dbo.maquina_cadastro t2
                    WHERE t2.maquina_id = t1.maquina_id AND t2.data_registro <= t1.data_registro
                    ORDER BY t2.data_registro DESC, t2.hora_registro DESC) as fabrica,
                    (SELECT TOP 1 t2.linha FROM AUTOMACAO.dbo.maquina_cadastro t2
                    WHERE t2.maquina_id = t1.maquina_id AND t2.data_registro <= t1.data_registro
                    ORDER BY t2.data_registro DESC, t2.hora_registro DESC) as linha,
                    t1.maquina_id,
                    t1.turno,
                    t1.contagem_total_ciclos,
                    t1.contagem_total_produzido,
                    (SELECT TOP 1 t2.produto_id FROM AUTOMACAO.dbo.maquina_produto t2
                    WHERE t2.maquina_id = t1.maquina_id AND t2.data_registro <= t1.data_registro
                    ORDER BY t2.data_registro DESC, t2.hora_registro DESC) as produto_id,
                    t1.data_registro,
                    t1.hora_registro,
                    ROW_NUMBER() OVER (
                        PARTITION BY t1.data_registro, t1.turno, t1.maquina_id
                        ORDER BY t1.data_registro DESC, t1.hora_registro DESC) AS rn
                FROM AUTOMACAO.dbo.maquina_info t1
            ) AS t
            INNER JOIN
                TOTVSDB.dbo.SB1000 SB1 WITH (NOLOCK)
                ON SB1.B1_FILIAL = '01' AND SB1.B1_COD = t.produto_id AND SB1.D_E_L_E_T_<>'*'
            WHERE t.rn = 1
                AND hora_registro > '00:01'
                AND data_registro between '{first_day}' and '{last_day}'
        """

        return query

    def execute_query(self, query):
        """
        Executes the given query and count query on the database and returns
        a response with the results.

        Args:
            query (str): The query to execute to get the records.
            request (Request): The request object.

        Returns:
            Response: The response containing the results, page, total pages, and total records.
        """
        try:
            with connections["sqlserver"].cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    return Response(
                        [],
                        status=status.HTTP_200_OK,
                    )

                df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])

                return df.to_dict("records")
        # pylint: disable=W0718
        except Exception as e:
            print(f"Erro na execução da query: {str(e)}")
            return Response(
                {"error": f"Database error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )