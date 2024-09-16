import pandas as pd
from data_prep.data_cleaning import data_cleaning
from data_prep.features_selection import feature_selection
from feature_extraction.features_extraction import feature_extraction
from feature_extraction.extract_increment import incremento
from clustering.clustering_execution import execute_clustering
from data_transformation.data_transformation import data_transformation
import logging

# Configuro il logger
logging.basicConfig(level=logging.INFO,  # Imposto il livello minimo di log
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Formato del log

# Caricamento del dataset
file_path = '../Teleassistance_Project/datasets/challenge_campus_biomedico_2024.parquet'
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
# logging.info(f" Dopo il clustering, il DataFrame ha {num_rows} righe e {num_columns} colonne.")




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


