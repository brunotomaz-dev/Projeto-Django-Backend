"""Módulo com os filtros do Django Rest Framework"""

import django_filters

from .models import InfoIHM, MaquinaIHM, MaquinaInfo, QualidadeIHM, QualProd


class MaquinaInfoFilter(django_filters.FilterSet):
    """Filtro de informações de máquina"""

    data_registro = django_filters.DateFilter(field_name="data_registro")

    class Meta:
        """Classe de metadados"""

        model = MaquinaInfo
        fields = {"data_registro": ["exact", "gt", "lt"]}


class MaquinaIHMFilter(django_filters.FilterSet):
    """Filtro de informações de IHM de máquina"""

    data_registro = django_filters.DateFilter(field_name="data_registro")

    class Meta:
        """Classe de metadados"""

        model = MaquinaIHM
        fields = {"data_registro": ["exact", "gt", "lt"]}


class InfoIHMFilter(django_filters.FilterSet):
    """Filtro de informações de máquina"""

    data_registro = django_filters.DateFilter(field_name="data_registro")

    class Meta:
        """Classe de metadados"""

        model = InfoIHM
        fields = {"data_registro": ["exact", "gt", "lt"]}


class QualidadeIHMFilter(django_filters.FilterSet):
    """Filtro de informações de máquina"""

    data_registro = django_filters.DateFilter(field_name="data_registro")

    class Meta:
        """Classe de metadados"""

        model = QualidadeIHM
        fields = {"data_registro": ["exact", "gt", "lt"]}


class QualProdFilter(django_filters.FilterSet):
    """Filtro de informações de máquina"""

    data_registro = django_filters.DateFilter(field_name="data_registro")

    class Meta:
        """Classe de metadados"""

        model = QualProd
        fields = {"data_registro": ["exact", "gt", "lt"]}
