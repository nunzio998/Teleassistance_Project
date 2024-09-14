import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, LabelEncoder
from clustering.clustering_analyzer import analyze_clustering
from clustering.clustering_metrics import compute_all_metrics


def remove_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove le feature poco significative dal DataFrame.
    :param df:
    :return: df senza le colonne specificate.

    #TODO da inserire in data cleaning/feature selection?

    """
    features_to_drop = [
        'id_prenotazione', 'id_paziente', 'asl_residenza', 'comune_residenza', 'descrizione_attivita',
        'asl_erogazione', 'codice_struttura_erogazione', 'provincia_residenza', 'provincia_erogazione',
        'struttura_erogazione', 'id_professionista_sanitario'
    ]
    return df.drop(columns=[col for col in features_to_drop if col in df.columns])

def define_features_types() -> (list, list):
    categorical_features = ['sesso', 'regione_residenza', 'regione_erogazione','tipologia_professionista_sanitario', 'incremento', 'tipologia_struttura_erogazione']
    numerical_features = ['eta_paziente', 'month', 'year']
    return categorical_features, numerical_features

def plot_elbow_method(data, max_clusters=10):

    inertia = []  # Lista per memorizzare l'inertia (somma delle distanze al quadrato dai centroidi)

    for n_clusters in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        kmeans.fit(data)
        inertia.append(kmeans.inertia_)  # Aggiungi l'inertia per il numero corrente di cluster


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

def transform_and_preprocess_data(df: pd.DataFrame, categorical_features: list, numerical_features: list):
    label_encoders = {}
    reverse_mapping = {}

    # Gestione delle colonne temporali (convertire in timestamp con utc=True)
    for col in ['data_contatto', 'data_erogazione']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            df[col] = df[col].astype('int64') // 10 ** 9  # Convert to UNIX timestamp safely

    # Applica LabelEncoder a ciascuna colonna categorica
    for col in categorical_features:
        if col in df.columns and (df[col].dtype == 'object' or isinstance(df[col].dtype, pd.StringDtype)):
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])
            label_encoders[col] = le
            # Aggiungi la mappatura inversa per ogni feature categorica
            reverse_mapping[col] = {i: label for i, label in enumerate(le.classes_)}

    # Verifica che tutte le colonne siano numeriche dopo l'encoding
    if not all(np.issubdtype(df[col].dtype, np.number) for col in df.columns):
        raise ValueError("Ci sono ancora colonne non numeriche nel DataFrame dopo l'encoding.")

    return df, label_encoders, reverse_mapping  # Restituisco anche il dizionario reverse_mapping



def apply_clustering(data, n_clusters=4, n_components=None):

    if n_components is None:
        n_components = min(10, data.shape[1])  # Imposta il numero massimo di componenti in base al numero di feature

    # Pipeline per il clustering con TruncatedSVD al posto di PCA
    pipeline = Pipeline(steps=[
        ('dim_reduction', TruncatedSVD(n_components=n_components)),  # Riduzione a 15 componenti principali (tanti quante sono le features)
        ('clustering', KMeans(n_clusters=n_clusters, random_state=42))  # Clustering con KMeans
    ])
    labels = pipeline.fit_predict(data)
    svd_data = pipeline.named_steps['dim_reduction'].transform(data)
    return labels, svd_data



def execute_clustering(df: pd.DataFrame, n_clusters=4):
    """
    Metodo che esegue tutti i metodi del file clustering_execution
    :param df:
    :return: df
    """
    print("Eseguo il clustering...")
    df_cleaned = remove_features(df)

    categorical_features, numerical_features = define_features_types()

    # Ora otteniamo anche reverse_mapping
    df_processed, label_encoders, reverse_mapping = transform_and_preprocess_data(df_cleaned, categorical_features, numerical_features)

    # Calcolo del numero ottimale di cluster
    print("Calcolo del numero ottimale di cluster...")
    plot_elbow_method(df_processed, max_clusters=10)

    # Applicazione del clustering
    print("Applicazione del clustering con KMeans...")
    labels, svd_data = apply_clustering(df_processed, n_clusters=n_clusters)

    # Aggiungiamo le etichette e le componenti principali al dataframe originale
    df_cleaned['Cluster'] = labels

    # Analisi del clustering, con la possibilità di usare reverse_mapping nei grafici
    analyze_clustering(df_cleaned, numerical_features, categorical_features, reverse_mapping)

    # Calcolo delle metriche e generazione dei plot, passando anche label_encoders e reverse_mapping
    print("Sto calcolando le metriche...")
    compute_all_metrics(df_cleaned, target_column='incremento', label_encoders=label_encoders)



    return df_cleaned, labels, svd_data









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
