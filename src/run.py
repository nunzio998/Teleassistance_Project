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
file_path = '../src/datasets/challenge_campus_biomedico_2024.parquet'
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

# STEP 6: Clustering Execution
df_clustered, cluster_labels, svd_transformed_data = execute_clustering(df, label_encoders, numerical_features,
                                                                        categorical_features, reverse_mapping)