import pandas as pd
from data_prep.data_cleaning import data_cleaning
from data_prep.features_selection import feature_selection_execution
from feature_extraction.features_extraction import feature_extraction
from feature_extraction.extract_increment import incremento
from clustering.clustering_execution import execute_clustering

# Configura pandas per visualizzare un DataFrame senza limiti di spazio
pd.set_option('display.max_rows', None)  # Nessun limite sul numero di righe
pd.set_option('display.max_columns', None)  # Nessun limite sul numero di colonne
pd.set_option('display.max_colwidth', None)  # Nessun limite sulla larghezza delle colonne
pd.set_option('display.expand_frame_repr', False)  # Non espande il DataFrame su più righe

# Caricamento del dataset
file_path = 'datasets/challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

#df = df.head(10000)

# Visualizzazione del numero di righe e colonne del dataset
num_rows, num_columns = df.shape
print(f" Inizialmente il DataFrame ha {num_rows} righe e {num_columns} colonne.")

# Data Cleaning
df = data_cleaning(df)
# Features Selection
df = feature_selection_execution(df)
# Feature extraction
df = feature_extraction(df)

# Visualizzazione del numero di righe e colonne del dataset
num_rows, num_columns = df.shape
print(f" Dopo la pulizia dei dati, il DataFrame ha {num_rows} righe e {num_columns} colonne.")

#df.to_csv('datasets/challenge_campus_biomedico_2024_imputed_selected_extracted.csv', index=False)

# Calcolo la variabile incremento
df = incremento(df)

# Elimina tutte le righe dove l'anno è 2019
df = df[df['year'] != 2019]
df.to_parquet('datasets/CMA.parquet', index=False)

# Eseguo il clustering
df_clustered, cluster_labels, svd_transformed_data = execute_clustering(df, n_clusters=4)

# Visualizzazione del numero di righe e colonne del dataset
num_rows, num_columns = df.shape
print(f" Dopo il clustering, il DataFrame ha {num_rows} righe e {num_columns} colonne.")




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

#TODO: Rimuovere feature
#TODO: Metriche di clustering
#TODO: Collogare Elbow method ad apply clustering
#TODO: Visualizzare i cluster
#TODO: Visualizzare le metriche di clustering
#TODO: Grafici generali

'''
Calcolo della purezza di ciascun cluster e della purezza complessiva:
Purezza di ciascun cluster:
Cluster 2: Purezza = 0.99
Cluster 0: Purezza = 0.53
Cluster 3: Purezza = 0.89
Cluster 1: Purezza = 1.00
Purezza complessiva: 0.83


Calcolo dell'indice di Silhouette... Attendi...
L'indice di Silhouette medio normalizzato è : 0.7405819883564095

Calcolo della metrica finale...
La metrica finale è : 0.5858758156998034
La metrica finale è: 0.59
'''
