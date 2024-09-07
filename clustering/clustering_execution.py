from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.cluster import KMeans
import seaborn as sns
import matplotlib.pyplot as plt

def encoding_features(df):

    """
    Metodo che effettua l'encoding delle feature categoriche
    :param df: dataframe
    :return: df, encoded_features, features_name

    """

    # Definisco le colonne delle feature categoriche
    categorical_features = ['asl_residenza', 'provincia_residenza', 'comune_residenza',
        'asl_erogazione', 'provincia_erogazione', 'struttura_erogazione', 'tipologia_struttura_erogazione',
        'tipologia_professionista_sanitario', 'sesso'
    ]

    df = df[df['year'] != 2019].copy()  # Creo una copia del DataFrame, eliminando i dati del 2019, poichè l'incremento è NaN

    df.reset_index(drop=True, inplace=True)  # Resetta l'indice

    # Preprocessing delle feature
    preprocessor = ColumnTransformer(transformers=[('cat', OneHotEncoder(), categorical_features)])

    # Trasformazione delle feature
    encoded_features = preprocessor.fit_transform(df.drop(['incremento', 'id_prenotazione', 'id_paziente'], axis=1))

    # Ottieni i nomi delle feature trasformate
    feature_names = preprocessor.get_feature_names_out()

    return df, encoded_features, feature_names


def kmeans_clustering(df, X):
    """
    Metodo che effettua il clustering attraverso l'algoritmo k-means
    :param df:
    :param X:
    :return:
    """
    # Numero di cluster basato sulle categorie di incremento
    n_clusters = len(df['incremento'].dropna().unique())  # Considero solo valori non NaN
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)

    # Fit e predizione dei cluster
    cluster_labels = kmeans.fit_predict(X)

    # Controllo che il numero di righe corrisponda
    if len(cluster_labels) != len(df):
        raise ValueError(
            f"Lunghezza delle etichette di cluster ({len(cluster_labels)}) non corrisponde alla lunghezza di df ({len(df)})")

    df.loc[:, 'cluster'] = cluster_labels  # Uso .loc per modificare il DataFrame


def cluster_plot(df):
    """
    Metodo che plotta la distribuzione dei cluster rispetto all'incremento
    :param df:
    :return:
    """
    plt.figure(figsize=(12, 8))
    sns.countplot(x='incremento', hue='cluster', data=df)
    plt.title('Distribuzione dei Cluster rispetto a Incremento')
    plt.show()


def execute_clustering(df):
    """
    Metodo che esegue tutti i metodi del file clustering_execution
    :param df:
    :return:

    """
    df, encoded_features, features_name = encoding_features(df)

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
