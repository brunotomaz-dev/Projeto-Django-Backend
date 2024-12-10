"""
Administração do Sistema SFM
"""

from django.contrib import admin

from .models import InfoIHM, MaquinaIHM, MaquinaInfo, QualidadeIHM, QualProd

# Register your models here.
admin.site.site_header = "Administração do Sistema SFM"
admin.site.register(MaquinaInfo)
admin.site.register(MaquinaIHM)
admin.site.register(InfoIHM)
admin.site.register(QualidadeIHM)
admin.site.register(QualProd)
