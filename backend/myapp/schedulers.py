"""Módulo que agenda a análise de dados"""

# schedulers.py
import logging
import threading

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from django.db import connections, transaction
from rest_framework.test import APIRequestFactory

from .data_analysis import InfoIHMJoin, join_qual_prod
from .models import InfoIHM, QualProd
from .views import (
    MaquinaIHMViewSet,
    MaquinaInfoProductionViewSet,
    MaquinaInfoViewSet,
    QualidadeIHMViewSet,
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


def _get_api_data(factory, endpoint, params, headers, view_set):
    """Obtém dados da API com tratamento de autorizações"""
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


def analisar_dados():
    """Função que será executada periodicamente"""
    with lock:
        try:
            factory = APIRequestFactory()
            access_token = get_jwt_token()
            headers = {
                "HTTP_AUTHORIZATION": f"Bearer {access_token}",
                "content_type": "application/json",
            }

            params = {"data_registro": pd.Timestamp("today").strftime("%Y-%m-%d")}
            # first_day = now.replace(day=1)
            # last_day = now.replace(day=pd.Period(now, "M").days_in_month)
            # days_ago_31 = now - pd.DateOffset(days=31)

            # Criar request com filtros
            # params = {
            #     "data_registro__gt": days_ago_31.strftime("%Y-%m-%d"),
            #     "data_registro__lt": last_day.strftime("%Y-%m-%d"),
            # }

            info_view = MaquinaInfoViewSet.as_view({"get": "list"})
            ihm_view = MaquinaIHMViewSet.as_view({"get": "list"})

            info_data = _get_api_data(factory, "/api/maquinainfo/", params, headers, info_view)
            ihm_data = _get_api_data(factory, "/api/maquinaihm/", params, headers, ihm_view)

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
            factory = APIRequestFactory()
            access_token = get_jwt_token()
            headers = {
                "HTTP_AUTHORIZATION": f"Bearer {access_token}",
                "content_type": "application/json",
            }

            today = pd.Timestamp("today").strftime("%Y-%m-%d")

            params = {"period": f"{today},{today}"}

            prod_view = MaquinaInfoProductionViewSet.as_view()
            qual_view = QualidadeIHMViewSet.as_view({"get": "list"})
            prod_data = _get_api_data(
                factory, "/api/maquinainfo/production/", params, headers, prod_view
            )
            qual_data = _get_api_data(factory, "/api/qualidade_ihm/", params, headers, qual_view)

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


def analisar_all_dados():
    """Função que será executada periodicamente"""
    analisar_dados()
    create_production_data()


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