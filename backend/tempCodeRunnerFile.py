 file_path = os.path.join(os.getcwd(), file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError("Arquivo não encontrado")

    print(f"Carregando dados do arquivo {file_path}...")
    df = pd.read_csv(file_path, delimiter=delimiter)
    print(f"Registros encontrados: {len(df)}")