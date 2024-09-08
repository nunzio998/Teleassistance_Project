import os

import pandas as pd
import numpy
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


def remove_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove le feature poco significative dal DataFrame.
    :param df:
    :return: df senza le colonne specificate.

    #TODO da inserire in data cleaning/feature selection? N.B. alcune cose servono per il calcolo dell'incremento

    """
    df.drop(columns=['id_prenotazione', 'data_nascita', 'asl_residenza',
                     'comune_residenza', 'descrizione_attivita',  'data_contatto', 'data_erogazione',
                     'asl_erogazione', 'ora_inizio_erogazione', 'ora_fine_erogazione', 'codice_struttura_erogazione'], inplace=True)
    return df

def define_categorical_features():
    """
    Definisce le colonne delle feature categoriche.
    :return: List of categorical features
    """
    return [
        'id_paziente', 'provincia_residenza', 'regione_residenza', 'provincia_erogazione',
        'regione_erogazione', 'tipologia_struttura_erogazione', 'tipologia_professionista_sanitario',
        'id_professionista_sanitario', 'sesso'
    ]

def define_numerical_features():
    """
    Definisce le colonne delle feature numeriche.
    :return: List of numerical features
    """
    return ['eta_paziente', 'durata_televisita']


def create_preprocessor(categorical_features, numerical_features):
    """
    Crea il ColumnTransformer per il preprocessing delle feature.
    Le feature numeriche rimangono invariate, in quanto sono già state normalizzate e standardizzate.
    Le feature numeriche vengono codificate con codifica OneHot.

    :param categorical_features: List of categorical features
    :param numerical_features: List of numerical features
    :return: ColumnTransformer
    """
    return ColumnTransformer(
        transformers=[
            ('num', 'passthrough', numerical_features),  # Mantiene le feature numeriche senza trasformarle
            ('cat', OneHotEncoder(sparse_output=True), categorical_features)
        ])


def plot_elbow_method(encoded_features, max_clusters=10):
    """
    Calcola e visualizza il metodo del gomito per determinare il numero ottimale di cluster.
    :param encoded_features: Array di feature codificate
    :param max_clusters: Numero massimo di cluster da testare
    """
    inertia = []  # Lista per memorizzare l'inertia (somma delle distanze al quadrato dai centroidi)

    for n_clusters in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        kmeans.fit(encoded_features)
        inertia.append(kmeans.inertia_)  # Aggiungi l'inertia per il numero corrente di cluster

    # Crea la cartella per salvare i grafici solo se non esiste
    if not os.path.exists('graphs'):
        os.makedirs('graphs')

    # Plot dell'Elbow Method
    plt.figure(figsize=(8, 6))
    plt.plot(range(1, max_clusters + 1), inertia, marker='o')
    plt.xlabel('Numero di Cluster')
    plt.ylabel('Inertia')
    plt.title('Metodo del Gomito')
    plt.grid(True)
    # Salva il grafico nella cartella 'graphs'
    plt.savefig('graphs/elbow_method.png')
    plt.close()

    # Ritorna i valori dell'inertia per ulteriori analisi
    return inertia

def transform_features(df, preprocessor):
    """
    Trasforma le feature del DataFrame e ottiene i nomi delle nuove feature.

    :param df: DataFrame originale
    :param preprocessor: ColumnTransformer
    :return: DataFrame trasformato, array di feature codificate, nomi delle feature
    """
    # Trasformazione delle feature
    encoded_features = preprocessor.fit_transform(df.drop(['incremento'], axis=1))

    # Ottieni i nomi delle feature trasformate
    feature_names = preprocessor.get_feature_names_out()

    return df, encoded_features, feature_names


def apply_clustering(encoded_features):
    """
    Applica il clustering sui dati trasformati utilizzando l'algoritmo K-Means.
    Riduciamo la dimensionalità del dataset a 15 componenti principali utilizzando TruncatedSVD ( è simile
    alla PCA, ma riesce a lavorare con matrici sparse (utile dopo la codifica OneHot)).

    :param encoded_features: Array di feature codificate
    :return: labels degli cluster, dati trasformati con TruncatedSVD

    """
    # Pipeline per il clustering con TruncatedSVD al posto di PCA
    pipeline = Pipeline(steps=[
        ('svd', TruncatedSVD(n_components=15)),  # Riduzione a 15 componenti principali (tanti quante sono le features)
        ('cluster', KMeans(n_clusters=4, random_state=42))  # Clustering con KMeans
    ])

    # Applicazione del clustering e della riduzione dimensionale
    pipeline.fit(encoded_features)

    # Otteniamo le etichette dei cluster
    labels = pipeline.named_steps['cluster'].labels_

    # Trasformiamo i dati con TruncatedSVD
    svd_data = pipeline.named_steps['svd'].transform(encoded_features)

    return labels, svd_data, encoded_features



def execute_clustering(df):
    """
    Metodo che esegue tutti i metodi del file clustering_execution
    :param df:
    :return: df

    """

    df = remove_features(df)

    categorical_features = define_categorical_features()

    numerical_features = define_numerical_features()

    preprocessor = create_preprocessor(categorical_features, numerical_features)

    df, encoded_features, feature_names = transform_features(df, preprocessor)

    labels, svd_data, encoded_features = apply_clustering(encoded_features)

    plot_elbow_method(encoded_features)

    return df








"""
Feature:

id_prenotazione
id_paziente
data_nascita
sesso
regione_residenza
asl_residenza
provincia_residenza
comune_residenza
descrizione_attivita
data_contatto
regione_erogazione
asl_erogazione   provincia_erogazione
struttura_erogazione
codice_struttura_erogazione
tipologia_struttura_erogazione
id_professionista_sanitario
tipologia_professionista_sanitario
data_erogazione
ora_inizio_erogazione
ora_fine_erogazione
eta_paziente
durata_televisita
year
month
incremento


Feature utili per il Clustering:

Regione Residenza
ASL Residenza
Provincia Residenza
Comune Residenza
\
Regione Erogazione
ASL Erogazione
Provincia Erogazione
Comune Erogazione
Struttura Erogazione / Codice Struttura Erogazione
Tipologia Struttura Erogazione ?
\
Professionista Sanitario
\
Sesso Paziente
Età paziente
Durata Televisita
Anno Televisita
Mese Televisita


Variabile target: Incremento

"""
