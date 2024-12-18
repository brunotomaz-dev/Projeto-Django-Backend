"""Módulo com variáveis globais e constantes"""

from enum import Enum

PESO_BANDEJAS = 0.028
PESO_SACO = 0.080

TEMPO_AJUSTE = 5

CICLOS_ESPERADOS = 11.2
CICLOS_BOLINHA = 7


class IndicatorType(Enum):
    """
    Enum para os tipos de indicadores.
    """

    EFFICIENCY = "eficiencia"  # cSpell: disable-line
    PERFORMANCE = "performance"
    REPAIR = "reparo"


# Dict de Descontos
DESC_EFF = {
    "Troca de Sabor": 15,
    "Troca de Produto": 35,
    "Refeição": 60,
    "Café e Ginástica Laboral": 10,
    "Treinamento": 60,
}
DESC_PERF = {
    "Troca de Sabor": 15,
    "Troca de Produto": 35,
    "Refeição": 60,
    "Café e Ginástica Laboral": 10,
    "Treinamento": 60,
}
DESC_REP = {"Troca de Produto": 35}

# List que não afeta ou afeta
NOT_EFF = ["Sem Produção", "Backup"]
NOT_PERF = [
    "Sem Produção",
    "Backup",
    "Limpeza para parada de Fábrica",
    "Risco de Contaminação",
    "Parâmetros de Qualidade",
    "Manutenção",
]
AF_REP = ["Manutenção", "Troca de Produtos"]
