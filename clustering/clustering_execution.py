from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.cluster import KMeans
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
import pandas as pd

def encoding_features(df):
    # Definisci le colonne categoriche
    categorical_features = [
        'asl_residenza', 'provincia_residenza', 'comune_residenza',
        'asl_erogazione', 'provincia_erogazione',
        'struttura_erogazione', 'tipologia_struttura_erogazione', 'tipologia_professionista_sanitario',
        'sesso'
    ]

    # Elimina i dati del 2019
    df = df[df['year'] != 2019].copy()  # Creiamo una copia esplicita del DataFrame
    print(f"Lunghezza di df dopo eliminazione del 2019: {len(df)}")

    # Resetta l'indice
    df.reset_index(drop=True, inplace=True)

    # Preprocessing delle feature
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(), categorical_features)
        ]
    )
    # Trasformazione delle feature
    encoded_features = preprocessor.fit_transform(df.drop(['incremento', 'id_prenotazione', 'id_paziente'], axis=1))

    # Ottieni i nomi delle feature trasformate
    feature_names = preprocessor.get_feature_names_out()
    print(f"Lunghezza di encoded_features: {encoded_features.shape[0]}")

    return df, encoded_features, feature_names


def kmeans_clustering(df, X):
    # Numero di cluster basato sulle categorie di incremento
    n_clusters = len(df['incremento'].dropna().unique())  # Assicurati di considerare solo valori non NaN
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)

    # Fit e predizione dei cluster
    cluster_labels = kmeans.fit_predict(X)

    # Assicurati che il numero di righe corrisponda
    if len(cluster_labels) != len(df):
        raise ValueError(
            f"Lunghezza delle etichette di cluster ({len(cluster_labels)}) non corrisponde alla lunghezza di df ({len(df)})")

    df.loc[:, 'cluster'] = cluster_labels  # Usa .loc per modificare il DataFrame

    # Verifica la lunghezza delle etichette
    print(f"Lunghezza delle etichette di cluster: {len(cluster_labels)}")
    print(f"Lunghezza di df['cluster']: {len(df['cluster'])}")


def cluster_plot(df):
    plt.figure(figsize=(12, 8))
    sns.countplot(x='incremento', hue='cluster', data=df)
    plt.title('Distribuzione dei Cluster rispetto a Incremento')
    plt.show()


def execute_clustering(df):
    # Elimina i dati del 2019 e resetta l'indice
    df, encoded_features, features_name = encoding_features(df)
    # Esegui PCA per visualizzare l'importanza delle feature

    kmeans_clustering(df, encoded_features)
    cluster_plot(df)



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
