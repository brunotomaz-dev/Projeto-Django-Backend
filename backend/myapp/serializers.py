"""Módulo que serializa os dados do Django Rest Framework"""

# cSpell: words serializável

from django.contrib.auth.models import User
from rest_framework import serializers
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

from .models import (
    AbsenceLog,
    Eficiencia,
    InfoIHM,
    MaquinaCadastro,
    MaquinaIHM,
    MaquinaInfo,
    Performance,
    PresenceLog,
    QualidadeIHM,
    QualProd,
    Repair,
)


class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    """Serializador de token"""

    def validate(self, attrs):
        data = super().validate(attrs)

        # Add informações adicionais
        data["first_name"] = self.user.first_name
        data["last_name"] = self.user.last_name
        data["groups"] = self.user.groups.values_list("name", flat=True)

        return data

    def create(self, validated_data):
        """Método create"""
        return User.objects.create(**validated_data)

    def update(self, instance, validated_data):
        """Método update"""
        instance.username = validated_data.get("username", instance.username)
        instance.first_name = validated_data.get("first_name", instance.first_name)
        instance.last_name = validated_data.get("last_name", instance.last_name)
        instance.email = validated_data.get("email", instance.email)
        instance.save()
        return instance


class RegisterSerializer(serializers.ModelSerializer):
    """Serializador de cadastro de usuário"""

    class Meta:
        """Classe de metadados"""

        model = User
        fields = ("username", "password", "email", "first_name", "last_name")
        extra_kwargs = {"password": {"write_only": True}}

    def create(self, validated_data):
        user = User.objects.create_user(
            username=validated_data["username"],
            password=validated_data["password"],
            email=validated_data["email"],
            first_name=validated_data["first_name"],
            last_name=validated_data["last_name"],
        )
        return user


class DynamicFieldsModelSerializer(serializers.ModelSerializer):
    """
    Serializer que permite selecionar campos dinamicamente
    """

    def __init__(self, *args, **kwargs):
        fields = kwargs.pop("fields", None)
        super().__init__(*args, **kwargs)

        if fields:
            allowed = set(fields)
            existing = set(self.fields)
            for field_name in existing - allowed:
                self.fields.pop(field_name)


class MaquinaInfoSerializer(DynamicFieldsModelSerializer):
    """Serializador de dados de informações de máquina"""

    class Meta:
        """Classe de metadados"""

        model = MaquinaInfo
        fields = "__all__"


class MaquinaInfoHourSerializer(serializers.Serializer):
    """Serializador de dados de informações de máquina"""

    maquina_id = serializers.CharField()
    intervalo = serializers.CharField()
    total = serializers.IntegerField()

    def to_representation(self, instance):
        """
        Converte a instância em uma representação serializável
        """
        if isinstance(instance, dict):
            return {
                "maquina_id": instance.get("maquina_id"),
                "intervalo": instance.get("intervalo"),
                "total": instance.get("total"),
            }
        return super().to_representation(instance)

    def create(self, validated_data):
        """Método create"""
        return validated_data

    def update(self, instance, validated_data):
        """Método update"""
        instance.update(validated_data)
        return instance


class MaquinaCadastroSerializer(serializers.ModelSerializer):
    """Serializador de dados de cadastro de máquina"""

    class Meta:
        """Classe de metadados"""

        model = MaquinaCadastro
        fields = "__all__"


class MaquinaIHMSerializer(serializers.ModelSerializer):
    """Serializador de dados de IHM de máquina"""

    s_backup = serializers.CharField(required=False, allow_null=True)
    fabrica = serializers.IntegerField(required=False)

    class Meta:
        """Classe de metadados"""

        model = MaquinaIHM
        fields = "__all__"


class InfoIHMSerializer(DynamicFieldsModelSerializer):
    """Serializador de dados de IHM de máquina"""

    class Meta:
        """Classe de metadados"""

        model = InfoIHM
        fields = "__all__"


class QualidadeIHMSerializer(serializers.ModelSerializer):
    """Serializador de dados de IHM de máquina"""

    class Meta:
        """Classe de metadados"""

        model = QualidadeIHM
        fields = "__all__"


class QualProdSerializer(DynamicFieldsModelSerializer):
    """Serializador de dados de produção de qualidade"""

    class Meta:
        """Classe de metadados"""

        model = QualProd
        fields = "__all__"


class EficienciaSerializer(DynamicFieldsModelSerializer):
    """Serializador de dados de eficiência"""

    class Meta:
        """Classe de metadados"""

        model = Eficiencia  # cSpell:words eficiencia
        fields = "__all__"


class PerformanceSerializer(DynamicFieldsModelSerializer):
    """Serializador de dados de performance"""

    class Meta:
        """Classe de metadados"""

        model = Performance
        fields = "__all__"


class RepairSerializer(DynamicFieldsModelSerializer):
    """Serializador de dados de reparo"""

    class Meta:
        """Classe de metadados"""

        model = Repair
        fields = "__all__"


class AbsenceLogSerializer(serializers.ModelSerializer):
    """Serializador dos dados de reparo"""

    class Meta:
        """Classe de Metadados"""

        model = AbsenceLog
        fields = "__all__"


class PresenceLogSerializer(serializers.ModelSerializer):
    """Serializador dos dados de reparo"""

    class Meta:
        """Classe de Metadados"""

        model = PresenceLog
        fields = "__all__"
