"""Módulo com variáveis globais e constantes"""

from enum import Enum

PESO_BANDEJAS = 0.028
PESO_SACO = 0.080

TEMPO_AJUSTE = 2

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
    "Refeição": 65,
    "Café e Ginástica Laboral": 10,
    "Treinamento": 60,
}
DESC_PERF = {
    "Troca de Sabor": 15,
    "Troca de Produto": 35,
    "Refeição": 65,
    "Café e Ginástica Laboral": 10,
    "Treinamento": 60,
}
DESC_REP = {
    "Troca de Produto": 35,
    "Manutenção Preventiva": 480,
    "Manutenção Corretiva Programada": 480,
}

# List que não afeta ou afeta
NOT_EFF = [
    "Sem Produção",
    "Backup",
    "Limpeza para parada de Fábrica",
    "Saída para backup",
    "Revezamento",
    "Manutenção Preventiva",
    "Manutenção Corretiva Programada",
]
NOT_PERF = [
    "Sem Produção",
    "Backup",
    "Limpeza para parada de Fábrica",
    "Risco de Contaminação",
    "Parâmetros de Qualidade",
    "Manutenção",
    "Saída para backup",
    "Revezamento",
    "Manutenção Preventiva",
    "Manutenção Corretiva Programada",
]
AF_REP = ["Manutenção", "Troca de Produtos"]
