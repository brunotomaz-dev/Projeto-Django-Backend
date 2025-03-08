"""Módulo de roteadores do Django para tabelas específicas"""


class SpecificTablesRouter:
    """Roteador para tabelas específicas"""

    specific_tables = [
        "maquina_info",
        "maquina_cadastro",
        "maquina_ihm",
        "analysis_info_ihm",
        "qualidade_ihm",
        "analysis_production",
        "analysis_eff",
        "analysis_perf",
        "analysis_repair",
        "analysis_absent",
        "analysis_presence",
    ]  # Adicione aqui as tabelas específicas

    def db_for_read(self, model, **hints):  # pylint: disable=unused-argument
        """Define quais bancos devem ser usados para ler as tabelas.

        Se a tabela estiver em self.specific_tables, retorna "sqlserver" (SQL Server),
        caso contrário, retorna None (tabela padrão sqlite3).
        """

        # pylint: disable=protected-access
        if model._meta.db_table in self.specific_tables:
            return "sqlserver"  # SQL Server
        return None

    # pylint: disable=unused-argument
    def db_for_write(self, model, instance=None, **hints):
        """Define quais bancos devem ser usados para gravar as tabelas.

        Se a tabela estiver em self.specific_tables, retorna "sqlserver" (SQL Server),
        caso contrário, retorna None.
        """

        # pylint: disable=protected-access
        if model._meta.db_table in self.specific_tables:
            return "sqlserver"  # SQL Server
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Define quais bancos devem ser usados para migrations.

        Se o banco for "default", permite fazer migrations.
        Caso contrário, não permite.
        """
        if db == "default":
            return True
        if model_name in self.specific_tables:
            return db == "sqlserver"
        return None
