import pandas as pd

file_path = "/mnt/c/Dati/Universita_Magistrale/1_Anno/Fondamenti_Intelligenza_Artificiale/project_FIA/Teleassistance_Project/challenge_campus_biomedico_2024.parquet"
# Caricamento dei dati dal file Parquet
df = pd.read_parquet(file_path)

# Visualizzazione delle prime righe del DataFrame
print(f"{df.head()}\n")
# Stampare la dimensione del dataset
print(f"Dimensione del dataset: {df.shape}\n")

# Assicurarsi che la colonna 'data_erogazione' sia di tipo datetime
df['data_erogazione'] = pd.to_datetime(df['data_erogazione'], utc=True)
# Ordinare i dati per data_erogazione
df = df.sort_values(by='data_erogazione')
# Stampare la dimensione del dataset ordinato in base alla data di erogazione delle teleassistenze
print(f"Dataset riordinato per data_erogazione: \n{df.head()}\n")

# Verificare la presenza di valori mancanti
missing_values = df.isnull().sum()
print(f"Valori mancanti in ciascuna colonna:\n{missing_values}")

# FEATURES MANCANTI:
# Eliminazione della feature data_disdetta
df.drop(columns=['data_disdetta'], inplace=True)
