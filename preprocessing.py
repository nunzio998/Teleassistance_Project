import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from datetime import datetime, timedelta

# Funzione per convertire il timestamp ISO 8601 in secondi dalla mezzanotte
def convert_timestamp_to_seconds(timestamp_str):
    if pd.isnull(timestamp_str):
        return None  # Gestisce i valori NaN
    timestamp = datetime.fromisoformat(timestamp_str)
    midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    return (timestamp - midnight).total_seconds()


file_path = "/mnt/c/Dati/Universita_Magistrale/1_Anno/Fondamenti_Intelligenza_Artificiale/project_FIA/Teleassistance_Project/challenge_campus_biomedico_2024.parquet"

# Caricamento dei dati dal file Parquet
df = pd.read_parquet(file_path)

# Rimozione delle colonne con un numero elevato di valori mancanti
df.drop(columns=['data_disdetta'], inplace=True)

# Conversione di 'ora_inizio_erogazione' e 'ora_fine_erogazione' in secondi dalla mezzanotte
df['ora_inizio_erogazione'] = df['ora_inizio_erogazione'].apply(convert_timestamp_to_seconds)
df['ora_fine_erogazione'] = df['ora_fine_erogazione'].apply(convert_timestamp_to_seconds)

# Label Encoding per le variabili categoriche
label_encoder = LabelEncoder()
for col in ['codice_provincia_residenza', 'codice_provincia_erogazione', 'comune_residenza']:
    df[col] = label_encoder.fit_transform(df[col].astype(str))

# Scaling dei dati
scaler = MinMaxScaler()
df_scaled = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)

# Imputazione KNN
knn_imputer = KNNImputer(n_neighbors=5)
df_imputed = pd.DataFrame(knn_imputer.fit_transform(df_scaled), columns=df_scaled.columns)

# Inverso del scaling per riportare i dati alla loro scala originale
df_final = pd.DataFrame(scaler.inverse_transform(df_imputed), columns=df_imputed.columns)

# Salva il dataset pulito
df_final.to_parquet('/mnt/c/Dati/Universita_Magistrale/1_Anno/Fondamenti_Intelligenza_Artificiale/project_FIA/Teleassistance_Project/cleaned_challenge_campus_biomedico_2024.parquet')






"""
# Visualizzazione delle prime righe del DataFrame
print(f"{df.head()}\n")
# Stampare la dimensione del dataset
print(f"Dimensione del dataset: {df.shape}\n")


# Assumi che df sia il tuo DataFrame
# Ottieni le prime N righe (es. 5) del DataFrame come stringa
df_head_string = df.head().to_string()
# Percorso del file dove vuoi salvare l'output
output_file_path = "/mnt/c/Dati/Universita_Magistrale/1_Anno/Fondamenti_Intelligenza_Artificiale/project_FIA/Teleassistance_Project/df.txt"

# Scrivi la stringa in un file
with open(output_file_path, 'w') as file:
    file.write(df_head_string)

# Esporta il DataFrame in un file CSV
output_path_csv = "/mnt/c/Dati/Universita_Magistrale/1_Anno/Fondamenti_Intelligenza_Artificiale/project_FIA/Teleassistance_Project/dataframe_output.csv"
df.to_csv(output_path_csv, index=False)
# Apri il file CSV esportato
subprocess.run(["open", file_path])

# Assicurarsi che la colonna 'data_erogazione' sia di tipo datetime
df['data_erogazione'] = pd.to_datetime(df['data_erogazione'], utc=True)
# Ordinare i dati per data_erogazione
df = df.sort_values(by='data_erogazione')
# Stampare la dimensione del dataset ordinato in base alla data di erogazione delle teleassistenze
#print(f"Dataset riordinato per data_erogazione: \n{df.head()}\n")

# Verificare la presenza di valori mancanti
missing_values = df.isnull().sum()
#print(f"Valori mancanti in ciascuna colonna:\n{missing_values}\n")

"""