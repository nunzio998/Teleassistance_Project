import pandas as pd
from data_prep.data_cleaning import data_cleaning
from data_prep.features_selection import feature_selection
from feature_extraction.features_extraction import feature_extraction
from feature_extraction.extract_increment import incremento
from clustering.clustering_execution import execute_clustering

# Configura pandas per visualizzare un DataFrame senza limiti di spazio
pd.set_option('display.max_rows', None)  # Nessun limite sul numero di righe
pd.set_option('display.max_columns', None)  # Nessun limite sul numero di colonne
pd.set_option('display.max_colwidth', None)  # Nessun limite sulla larghezza delle colonne
pd.set_option('display.expand_frame_repr', False)  # Non espande il DataFrame su più righe

# Caricamento del dataset
file_path = 'challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

# STEP 1: Data Cleaning
df = data_cleaning(df)

# STEP 2: Features Selection
df = feature_selection(df)

# STEP 3: Feature extraction
df = feature_extraction(df)

# STEP 4: Calcolo dell'incremento
df = incremento(df)

# STEP 5: Data Transformation
df, label_encoders, reverse_mapping, numerical_features, categorical_features = data_transformation(df)

# STEP 5: Clustering Execution
df_clustered, cluster_labels, svd_transformed_data = execute_clustering(df, label_encoders, numerical_features,
                                                                        categorical_features, reverse_mapping)

# # Visualizzazione del numero di righe e colonne del dataset
# num_rows, num_columns = df.shape
# print(f" Dopo il clustering, il DataFrame ha {num_rows} righe e {num_columns} colonne.")




'''
Statistiche valori mancanti dopo l'imputazione:

codice_provincia_residenza      28380
comune_residenza                  135 --> da non toccare perché relativi al comune di None in provincia di Torino
codice_provincia_erogazione     28776
ora_inizio_erogazione           23652
ora_fine_erogazione             23652
data_disdetta                  460639
'''


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

#TODO
1. Cambio gli if per il valore dell'incremento


'''


