""" Módulo de criação de modelos do Django """

# cSpell:ignore usuario
from django.db import models


# Create your models here.
class MaquinaInfo(models.Model):
    """Modelo de informações de máquina"""

    recno = models.AutoField(primary_key=True)
    maquina_id = models.CharField(max_length=50)
    status = models.CharField(max_length=50)
    ciclo_1_min = models.FloatField()
    ciclo_15_min = models.FloatField()
    contagem_total_ciclos = models.FloatField()
    contagem_total_produzido = models.FloatField()
    turno = models.CharField(max_length=3)
    data_registro = models.DateField()
    hora_registro = models.TimeField()
    tempo_parada = models.FloatField()
    tempo_rodando = models.FloatField()

    class Meta:
        """Definição do nome da tabela"""

        db_table = "maquina_info"

    def __str__(self):
        return f"{self.maquina_id} - {self.status} - {self.data_registro} - {self.hora_registro}"


class MaquinaCadastro(models.Model):
    """Modelo de cadastro de máquina"""

    recno = models.AutoField(primary_key=True)
    maquina_id = models.CharField(max_length=50)
    fabrica = models.CharField(max_length=50)
    linha = models.SmallIntegerField()
    data_registro = models.DateField()
    hora_registro = models.TimeField()
    usuario_id = models.CharField(max_length=9)

    class Meta:
        """Definição do nome da tabela"""

        db_table = "maquina_cadastro"

    def __str__(self):
        return f"{self.maquina_id} - {self.linha} - {self.data_registro} - {self.hora_registro}"


class MaquinaIHM(models.Model):
    """Modelo de informações de IHM de máquina"""

    recno = models.AutoField(primary_key=True)
    linha = models.SmallIntegerField()
    maquina_id = models.CharField(max_length=50)
    motivo = models.CharField(max_length=50)
    equipamento = models.CharField(max_length=50)
    problema = models.CharField(max_length=256)
    causa = models.CharField(max_length=256)
    os_numero = models.CharField(max_length=9)
    operador_id = models.CharField(max_length=9)
    data_registro = models.DateField()
    hora_registro = models.TimeField()

    class Meta:
        """Definição do nome da tabela"""

        db_table = "maquina_ihm"

    def __str__(self):
        return f"{self.linha} - {self.motivo} - {self.data_registro} - {self.hora_registro}"


class InfoIHM(models.Model):
    """Modelo de informações de IHM de máquina"""

    recno = models.AutoField(primary_key=True)
    fabrica = models.SmallIntegerField()
    linha = models.SmallIntegerField()
    maquina_id = models.CharField(max_length=50)
    turno = models.CharField(max_length=3)
    status = models.CharField(max_length=10)
    data_registro = models.DateField()
    hora_registro = models.TimeField()
    motivo = models.CharField(max_length=50, null=True)
    equipamento = models.CharField(max_length=50, null=True)
    problema = models.CharField(max_length=256, null=True)
    causa = models.CharField(max_length=256, null=True)
    os_numero = models.CharField(max_length=9, null=True)
    operador_id = models.CharField(max_length=9, null=True)
    data_registro_ihm = models.DateField(null=True)
    hora_registro_ihm = models.TimeField(null=True)
    s_backup = models.CharField(max_length=50, null=True)
    data_hora = models.DateTimeField()
    data_hora_final = models.DateTimeField()
    tempo = models.IntegerField()

    class Meta:
        """Definição do nome da tabela"""

        db_table = "analysis_info_ihm"
        indexes = [models.Index(fields=["data_registro"])]

    def __str__(self):
        return f"{self.linha} - {self.status} - {self.data_registro} - {self.hora_registro}"


class QualidadeIHM(models.Model):
    """Modelo de informações de qualidade de máquina"""

    recno = models.AutoField(primary_key=True)
    linha = models.SmallIntegerField()
    maquina_id = models.CharField(max_length=50)
    bdj_vazias = models.FloatField(null=True)
    bdj_retrabalho = models.FloatField(null=True)
    descarte_paes = models.FloatField(null=True)  # cSpell: disable-line
    descarte_paes_pasta = models.FloatField(null=True)  # cSpell: disable-line
    descarte_pasta = models.FloatField(null=True)
    data_registro = models.DateField()
    hora_registro = models.TimeField()

    class Meta:
        """Definição do nome da tabela"""

        db_table = "qualidade_ihm"
        indexes = [models.Index(fields=["data_registro"])]

    def __str__(self):
        return f"{self.linha} - {self.data_registro}"


class QualProd(models.Model):
    """Modelo de informações de qualidade de máquina"""

    recno = models.AutoField(primary_key=True)
    linha = models.SmallIntegerField()
    maquina_id = models.CharField(max_length=50)
    turno = models.CharField(max_length=3)
    data_registro = models.DateField()
    produto = models.CharField(max_length=128)
    total_ciclos = models.SmallIntegerField()
    total_produzido_sensor = models.SmallIntegerField()
    bdj_vazias = models.SmallIntegerField()
    bdj_retrabalho = models.SmallIntegerField()
    total_produzido = models.SmallIntegerField()
    descarte_paes = models.FloatField(null=True)  # cSpell: disable-line
    descarte_paes_pasta = models.FloatField(null=True)  # cSpell: disable-line
    descarte_pasta = models.FloatField(null=True)

    class Meta:
        """Definição do nome da tabela"""

        db_table = "analysis_production"
        indexes = [models.Index(fields=["data_registro"])]

    def __str__(self):
        return f"{self.linha} - {self.data_registro} - {self.produto} - {self.total_produzido}"
