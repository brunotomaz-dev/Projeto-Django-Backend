"""Módulo de URLs do Django Rest Framework"""

# cSpell:ignore maquinainfo maquinaihm

from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenRefreshView, TokenVerifyView

from .views import (
    AbsenceViewSet,
    CustomTokenObtainPairView,
    EficienciaViewSet,
    InfoIHMViewSet,
    MaquinaIHMViewSet,
    MaquinaInfoProductionViewSet,
    MaquinaInfoViewSet,
    PerformanceViewSet,
    QualidadeIHMViewSet,
    QualProdViewSet,
    RegisterView,
    RepairViewSet,
    change_password,
)

router = DefaultRouter()
router.register(r"maquinainfo", MaquinaInfoViewSet)
router.register(r"maquinaihm", MaquinaIHMViewSet)
router.register(r"info_ihm", InfoIHMViewSet)
router.register(r"qualidade_ihm", QualidadeIHMViewSet)
router.register(r"qual_prod", QualProdViewSet)
router.register(r"eficiencia", EficienciaViewSet)  # cSpell:words eficiencia
router.register(r"performance", PerformanceViewSet)
router.register(r"repair", RepairViewSet)
router.register(r"absenteismo", AbsenceViewSet)  # cSpell: words absenteismo

urlpatterns = [
    path("token/", CustomTokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("register/", RegisterView.as_view(), name="register"),
    path("change-password/", change_password, name="change-password"),
    path("token/verify/", TokenVerifyView.as_view(), name="token_verify"),
    path(
        "maquinainfo/production/",
        MaquinaInfoProductionViewSet.as_view(),
        name="maquinainfo_production",
    ),
    path("", include(router.urls)),
]
