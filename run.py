import pandas as pd
from data_prep.data_cleaning import data_cleaning
from data_prep.features_selection import feature_selection_execution
from feature_extraction.features_extraction import feature_extraction
from feature_extraction.extract_increment import incremento
from clustering.clustering_execution import execute_clustering

# Caricamento del dataset
file_path = 'datasets/challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)
#df = df.head(10000)
# Visualizzazione del numero di righe e colonne del dataset
num_rows, num_columns = df.shape
print(f"Il DataFrame ha {num_rows} righe e {num_columns} colonne.")

# Data Cleaning
df = data_cleaning(df)
# Features Selection
df = feature_selection_execution(df)
# Feature extraction
df = feature_extraction(df)

#df.to_csv('datasets/challenge_campus_biomedico_2024_imputed_selected_extracted.csv', index=False)
df=incremento(df)
execute_clustering(df)

#Visualizzazione di 20 righe del dataset casuali dopo aver aggiunto la colonna incremento
df_sample = df.sample(n=100, random_state=1)
# Visualizza le prime 500 righe del DataFrame
print(df_sample.to_string())

# Visualizzazione del numero di righe e colonne del dataset
num_rows, num_columns = df.shape
print(f"Il DataFrame ha {num_rows} righe e {num_columns} colonne.")


'''
Statistiche valori mancanti dopo l'imputazione:

codice_provincia_residenza      28380
comune_residenza                  135 --> da non toccare perché relativi al comune di None in provincia di Torino
codice_provincia_erogazione     28776
ora_inizio_erogazione           23652
ora_fine_erogazione             23652
data_disdetta                  460639
'''

# TODO: rimuovere colonne relative a 'codice_provincia_residenza' e 'codice_provincia_erogazione' in quanto non utili

# TODO: rimuovere tutti i campioni che hanno una data_disdetta non nulla poiché non significativi dato che la
#  televisita non è avvenuta ma è stata annullata. In questo modo non ci saranno valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione'

# TODO: rimuovere colonne relative a 'data_disdetta' in quanto non utili. Dopo il secondo TODO per quella feature ci saranno solo valori mancanti

"""
DOPO TODO:
Statistiche valori mancanti dopo l'imputazione:

codice_provincia_residenza      27016
comune_residenza                  130
codice_provincia_erogazione     27396
data_disdetta                  460639
dtype: int64
-----------------------------------
"""
