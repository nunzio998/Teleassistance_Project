import pandas as pd
from data_prep.data_cleaning import data_cleaning
from data_prep.features_selection import feature_selection_execution
from feature_extraction.features_extraction import feature_extraction
from feature_extraction.extract_increment import incremento
from clustering.clustering_execution import execute_clustering

# Caricamento del dataset
file_path = 'datasets/challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

# STEP 1: Data Cleaning
df = data_cleaning(df)

# STEP 2: Features Selection
df = feature_selection_execution(df)

# STEP 3: Feature extraction
df = feature_extraction(df)

# STEP 4: Calcolo dell'incremento
df = incremento(df)

# STEP 5: Clustering Execution
df_clustered, cluster_labels, svd_transformed_data = execute_clustering(df , n_clusters=4)









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

#TODO
1. Cambio gli if per il valore dell'incremento


'''