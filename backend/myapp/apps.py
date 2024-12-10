"""Arquivo de configuração do aplicativo"""

import os
import sys
import threading
from pathlib import Path

from django.apps import AppConfig

lock = threading.Lock()


class MyappConfig(AppConfig):
    """Classe de configuração do aplicativo"""

    default_auto_field = "django.db.models.BigAutoField"
    name = "myapp"
    verbose_name = "API de Coleta de Dados de Máquinas"

    def ready(self):
        """
        Função chamada quando o aplicativo está pronto.
        """
        if all(
            [
                "runserver" in sys.argv,  # Apenas no runserver
                Path(sys.argv[0]).stem != "pytest",  # Não durante testes
                os.environ.get("RUN_MAIN", None) == "true",  # Apenas no processo principal
            ]
        ):
            # pylint: disable=import-outside-toplevel
            from .schedulers import start_scheduler

            threading.Thread(target=start_scheduler).start()
