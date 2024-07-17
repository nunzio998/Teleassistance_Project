import pandas as pd
from data_prep.imputation import imputate_missing_values
from data_prep.feature_selection import remove_columns_with_unique_correlation


# Caricamento del dataset
file_path = 'datasets/challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

# Imputazione dei valori mancanti
df = imputate_missing_values(df)

# Rimozione delle colonne con correlazione univoca
df = remove_columns_with_unique_correlation(df)