"""Módulo de URLs do Django Rest Framework"""

# cSpell:ignore maquinainfo maquinaihm

from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenRefreshView, TokenVerifyView

from .views import (
    CustomTokenObtainPairView,
    InfoIHMViewSet,
    MaquinaIHMViewSet,
    MaquinaInfoProductionViewSet,
    MaquinaInfoViewSet,
    QualidadeIHMViewSet,
    QualProdViewSet,
    RegisterView,
)

router = DefaultRouter()
router.register(r"maquinainfo", MaquinaInfoViewSet)
router.register(r"maquinaihm", MaquinaIHMViewSet)
router.register(r"info_ihm", InfoIHMViewSet)
router.register(r"qualidade_ihm", QualidadeIHMViewSet)
router.register(r"qual_prod", QualProdViewSet)


urlpatterns = [
    path("token/", CustomTokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("register/", RegisterView.as_view(), name="register"),
    path("token/verify/", TokenVerifyView.as_view(), name="token_verify"),
    path(
        "maquinainfo/production/",
        MaquinaInfoProductionViewSet.as_view(),
        name="maquinainfo_production",
    ),
    path("", include(router.urls)),
]
