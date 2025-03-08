import os
from datetime import datetime

import django
import pandas as pd

# Configurar ambiente django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sfm.settings")
django.setup()

# importar os modelos django
# Flake8: noqa
# pylint: disable=C0413
from django.db import transaction
from myapp.models import PresenceLog


# Ler o arquivo de absenteísmo
def load_dataframe(file_path, delimiter=","):

    file_path = os.path.join(os.getcwd(), file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError("Arquivo não encontrado")

    print(f"Carregando dados do arquivo {file_path}...")
    df = pd.read_csv(file_path, delimiter=delimiter)
    print(f"Registros encontrados: {len(df)}")

    print("\nÚltimos 5 registros:")
    print(df.tail())

    return df


def process_row(row):

    data_reg = datetime.strptime(row["Data"], "%Y-%m-%d").date()
    hora_reg = datetime.strptime(row["Hora"], "%H:%M:%S").time()

    return PresenceLog.objects.update_or_create(  # pylint: disable=no-member
        panificacao=row["Panificação"],
        forno=row["Forno"],
        pasta=row["Pasta"],
        recheio=row["Recheio"],
        embalagem=row["Embalagem"],
        lideranca=row["Pães Diversos"],
        turno=row["Turno"],
        defaults={
            "hora_registro": hora_reg,
            "data_registro": data_reg,
            "usuario": row["Usuario"],
        },
    )


def process_single_record(row, records_created):
    """
    Process a single record row and update the count of created records.

    Args:
        row: A row of data to be processed
        records_created (int): Current count of successfully created records

    Returns:
        tuple: A tuple containing:
            - int: Updated count of records created
            - int: Status code (0 for success, 1 for failure)

    The function attempts to process a single row of data and increments the records_created
    counter if successful. It prints progress updates for every 100 records created.
    If an error occurs during processing, it prints error details and returns the
    current records count with a failure status code.
    """
    try:
        _, created = process_row(row)
        if created:
            records_created += 1
            if records_created % 100 == 0:
                print(f"Progresso: {records_created} registros criados...")
            return records_created, 0
        return records_created, 1
    except Exception as e:  # pylint: disable=W0718
        print(f"Erro ao processar linha: {row}")
        print(f"Erro: {str(e)}")
        return records_created, 1


def process_records(records):
    """Processes a list of records, creating new entries and skipping duplicates.
    Args:
        records (list): A list of records to be processed.
                        Each record is expected to be a dictionary-like object
                         containing the necessary data for creating a new entry.
    Returns:
        tuple: A tuple containing two integers:
               - The number of records successfully created.
               - The number of records skipped due to duplication or other issues.
    """

    records_created = records_skipped = 0

    for row in records:
        records_created, skipped = process_single_record(row, records_created)
        records_skipped += skipped

    return records_created, records_skipped


def import_presences(file_path):
    df = load_dataframe(file_path)

    with transaction.atomic():
        records_created, records_skipped = process_records(df.to_dict("records"))

    print(f"Importação concluída: {records_created} registros criados, {records_skipped} ignorados")
    return records_created, records_skipped


if __name__ == "__main__":
    # Use o nome correto do arquivo
    import_presences("./backend/registro_presenca.csv")
