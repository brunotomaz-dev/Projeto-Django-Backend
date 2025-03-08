"""Módulo que agenda a análise de dados"""

# schedulers.py
import logging
import threading

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from django.db import connections, models, transaction
from rest_framework.test import APIRequestFactory

from .data_analysis import InfoIHMJoin, ProductionIndicators, join_qual_prod
from .models import Eficiencia, InfoIHM, Performance, QualProd, Repair  # cSpell:words eficiencia
from .utils import IndicatorType
from .views import (
    InfoIHMViewSet,
    MaquinaIHMViewSet,
    MaquinaInfoProductionViewSet,
    MaquinaInfoViewSet,
    QualidadeIHMViewSet,
    QualProdViewSet,
)

logger = logging.getLogger(__name__)
lock = threading.Lock()


def get_jwt_token():
    """
    Obtém o token JWT necessário para realizar requisições
    autenticadas na API.

    Retorna:
        str: Token JWT
    """

    url = "http://localhost:8000/api/token/"
    data = {"username": "scheduler.admin", "password": "JRZR.qCh6:Qk_3D"}  # cSpell: disable-line
    response = requests.post(url, data=data, timeout=10)
    if response.status_code == 200:
        token_data = response.json()
        return token_data["access"]

    raise requests.exceptions.RequestException("Não foi possível obter o token JWT")


def get_new_access_token(refresh_token):
    """
    Obtém um novo token de acesso usando o token de refresh.

    Retorna:
        str: Novo token de acesso
    """

    url = "http://localhost:8000/api/token/refresh/"
    data = {"refresh": refresh_token}
    response = requests.post(url, data=data, timeout=10)
    if response.status_code == 200:
        return response.json()["access"]

    raise requests.exceptions.RequestException("Não foi possível obter um novo token de acesso")


def _get_api_data(endpoint, params, view_set):
    """Obtém dados da API com tratamento de autorizações"""
    # Acessa o token
    access_token = get_jwt_token()
    # Ajusta o headers com o token
    headers = {
        "HTTP_AUTHORIZATION": f"Bearer {access_token}",
        "content_type": "application/json",
    }
    # Faz a requisição
    factory = APIRequestFactory()
    request = factory.get(endpoint, params, **headers)
    response = view_set(request)

    if response.status_code == 401:
        new_access_token = get_new_access_token(headers["HTTP_AUTHORIZATION"].split()[1])
        headers["HTTP_AUTHORIZATION"] = f"Bearer {new_access_token}"
        request = factory.get(endpoint, params, **headers)
        response = view_set(request)

    if hasattr(response, "data"):
        data = pd.DataFrame(response.data)
        if data.empty:
            raise ValueError(f"Dados vazios recebidos da API: {endpoint}")
        return data
    return pd.DataFrame()


def _save_processed_data(dados_processados):
    """Salva os dados processados no banco de dados"""
    with transaction.atomic():
        for dado in dados_processados.to_dict("records"):
            InfoIHM.objects.update_or_create(  # pylint: disable=no-member
                maquina_id=dado["maquina_id"],
                data_registro=dado["data_registro"],
                hora_registro=dado["hora_registro"],
                defaults=dado,
            )


DATA_ANALYSIS = "2025-03-06"


def today_date():
    """Função que retorna a data de hoje"""
    return pd.Timestamp("today").strftime("%Y-%m-%d")


def analisar_dados():
    """Função que será executada periodicamente"""
    with lock:
        try:

            # Criar request com filtros
            # params = {"data_registro": today_date()}

            params = {
                "data_registro__gte": DATA_ANALYSIS,
                "data_registro__lte": today_date(),
            }

            info_view = MaquinaInfoViewSet.as_view({"get": "list"})
            ihm_view = MaquinaIHMViewSet.as_view({"get": "list"})

            info_data = _get_api_data("/api/maquinainfo/", params, info_view)
            ihm_data = _get_api_data("/api/maquinaihm/", params, ihm_view)

            if not info_data.empty and not ihm_data.empty:
                info_ihm_join = InfoIHMJoin(ihm_data, info_data)
                dados_processados = info_ihm_join.join_data()
                _save_processed_data(dados_processados)

        except (ConnectionError, ValueError, KeyError) as e:
            logger.error("Erro ao analisar dados: %s", str(e))
        finally:
            connections.close_all()


def create_production_data():
    """
    Função que cria dados de produção.

    Obtém os dados de produção e qualidade da API,
    junta-os e salva no banco de dados.

    Note que essa função é executada periodicamente via scheduler.
    """
    with lock:
        try:
            today = today_date()

            # params = {"period": f"{today},{today}"}
            params = {"period": f"{DATA_ANALYSIS},{today}"}

            prod_view = MaquinaInfoProductionViewSet.as_view()
            qual_view = QualidadeIHMViewSet.as_view({"get": "list"})
            prod_data = _get_api_data("/api/maquinainfo/production/", params, prod_view)
            qual_data = _get_api_data("/api/qualidade_ihm/", params, qual_view)

            if not prod_data.empty and not qual_data.empty:

                dados_processados = join_qual_prod(prod_data, qual_data)

                with transaction.atomic():
                    for dado in dados_processados.to_dict("records"):
                        QualProd.objects.update_or_create(  # pylint: disable=no-member
                            maquina_id=dado["maquina_id"],
                            data_registro=dado["data_registro"],
                            turno=dado["turno"],
                            defaults=dado,
                        )

        except (ConnectionError, ValueError, KeyError) as e:
            logger.error("Erro ao criar dados de produção: %s", str(e))
        finally:
            connections.close_all()


def __update_ind_db(df: pd.DataFrame, model: models.Model):
    """Função auxiliar para atualizar os indicadores no banco de dados"""
    with transaction.atomic():
        for row in df.to_dict("records"):
            model.objects.update_or_create(  # pylint: disable=no-member
                maquina_id=row["maquina_id"],
                data_registro=row["data_registro"],
                turno=row["turno"],
                defaults=row,
            )


def create_indicators():
    """
    Função que cria indicadores de eficiência, performance e reparo.

    Obtém os dados de produção e qualidade da API,
    junta-os e calcula os indicadores.

    Note que essa função é executada periodicamente via scheduler.
    """
    with lock:
        try:
            today = today_date()
            # Define os parâmetros
            # params = {"data_registro": today}

            # params = {"data_registro": DATA_ANALYSIS}

            params = {
                "data_registro__gte": DATA_ANALYSIS,
                "data_registro__lte": today,
            }

            # Faz a requisição de dados
            production = QualProdViewSet.as_view({"get": "list"})
            info_ihm = InfoIHMViewSet.as_view({"get": "list"})
            prod_data = _get_api_data("/api/qual_prod/", params, production)
            info_data = _get_api_data("/api/info_ihm/", params, info_ihm)

            if not prod_data.empty and not info_data.empty:
                create_ind = ProductionIndicators().create_indicators
                # Criar os indicadores de eficiência, performance e reparo

                # Eficiência
                eff_ind = create_ind(
                    info=info_data, prod=prod_data, indicator=IndicatorType.EFFICIENCY
                )
                # Salvar os indicadores no banco de dados usando transações atômicas
                __update_ind_db(eff_ind, Eficiencia)
                # Performance
                perf_ind = create_ind(
                    info=info_data, prod=prod_data, indicator=IndicatorType.PERFORMANCE
                )
                # Salvar os indicadores no banco de dados usando transações atômicas
                __update_ind_db(perf_ind, Performance)
                # Reparo
                repair_ind = create_ind(
                    info=info_data, prod=prod_data, indicator=IndicatorType.REPAIR
                )
                # Salvar os indicadores no banco de dados usando transações atômicas
                __update_ind_db(repair_ind, Repair)
        # Se ocorrer algum erro, loga o erro
        except (ConnectionError, ValueError, KeyError) as e:
            logger.error("Erro ao criar indicadores: %s", str(e))
        # Garante que as conexões com o banco de dados são fechadas independentemente do resultado
        finally:
            connections.close_all()


def analisar_all_dados():
    """Função que será executada periodicamente"""
    analisar_dados()
    create_production_data()
    create_indicators()
    # print("----------------------------- Concluído -----------------------------")


# cSpell:ignore jobstore periodica
def start_scheduler():
    """Inicializa o scheduler"""
    with lock:
        try:
            scheduler = BackgroundScheduler()

            if not scheduler.get_job("analise_periodica"):
                # Adiciona job para executar a cada minuto
                scheduler.add_job(
                    analisar_all_dados,
                    "interval",
                    minutes=1,
                    name="analise_periodica",
                    jobstore="default",
                )

            scheduler.start()
            logger.info("Scheduler iniciou com sucesso")

        except (ValueError, TypeError, ImportError) as e:
            logger.error("Erro ao iniciar o scheduler: %s", e)
