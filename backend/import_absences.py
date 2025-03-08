"""
This script imports absence data from a CSV file into a Django database.
The script defines several functions to:
- Load data from a CSV file into a pandas DataFrame.
- Process each row of the DataFrame to create or update AbsenceLog entries in the database.
- Handle potential errors during data processing.
- Track the number of records created and skipped.
The script uses Django's ORM and transaction management to ensure data integrity.
"""

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
from myapp.models import AbsenceLog


# Ler o arquivo de absenteísmo
def load_dataframe(file_path, delimiter=","):
    """
    Load data from a CSV file into a pandas DataFrame.
    This function loads a CSV file into a pandas DataFrame, stripping whitespace
    from string values and replacing NaN values with empty strings.
    Parameters:
    -----------
    file_path : str
        Relative path to the CSV file to be loaded.
    delimiter : str, optional
        The delimiter used in the CSV file. Default is ",".
    Returns:
    --------
    pandas.DataFrame
        DataFrame with the loaded data.
    Raises:
    -------
    FileNotFoundError
        If the specified file does not exist.
    Notes:
    ------
    The file path is considered relative to the current working directory.
    """

    file_path = os.path.join(os.getcwd(), file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError("Arquivo não encontrado")

    print(f"Carregando dados do arquivo {file_path}...")
    df = pd.read_csv(file_path, delimiter=delimiter)
    print(f"Registros encontrados: {len(df)}")

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df.where(pd.notnull(df), "")


def process_row(row):
    """
    Processes a row of absence data and updates or creates an AbsenceLog entry.
    Args:
        row (dict): A dictionary containing the following keys:
            - "Data" (str): The date of the absence in the format "%Y-%m-%d".
            - "Hora" (str): The time of the absence in the format "%H:%M:%S".
            - "Setor" (str): The sector of the employee.
            - "Turno" (str): The shift of the employee.
            - "Nome" (str): The name of the employee.
            - "Tipo" (str): The type of absence.
            - "Motivo" (str or NaN): The reason for the absence.
            - "Usuario" (str): The user who recorded the absence.
    Returns:
        tuple: A tuple containing the AbsenceLog object and a boolean indicating
               whether the object was created (True) or updated (False).
    """

    data_reg = datetime.strptime(row["Data"], "%Y-%m-%d").date()
    hora_reg = datetime.strptime(row["Hora"], "%H:%M:%S").time()

    return AbsenceLog.objects.update_or_create(  # pylint: disable=no-member
        setor=row["Setor"],
        turno=row["Turno"],
        nome=row["Nome"],
        tipo=row["Tipo"],
        defaults={
            "motivo": row["Motivo"] if not pd.isna(row["Motivo"]) else "",
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


def import_absences(file_path, delimiter=","):
    """
    Import absences from a CSV or similar delimited file into the database.
    This function reads a file containing absence data, processes each record,
    and imports them into the database within a single transaction. It tracks
    how many records were successfully created and how many were skipped.
    Parameters:
    ----------
    file_path : str
        Path to the file containing absence data to be imported
    delimiter : str, optional
        Character used as field separator in the file (default is ",")
    Returns:
    -------
    tuple
        A tuple containing (records_created, records_skipped) counts
    Notes:
    -----
    The function uses transaction.atomic() to ensure database consistency.
    If any error occurs during processing, all database changes will be rolled back.
    """

    df = load_dataframe(file_path, delimiter)

    with transaction.atomic():
        records_created, records_skipped = process_records(df.to_dict("records"))

    print(f"Importação concluída: {records_created} registros criados, {records_skipped} ignorados")
    return records_created, records_skipped


if __name__ == "__main__":
    # Use o nome correto do arquivo
    import_absences("./backend/absenteismo.csv")
