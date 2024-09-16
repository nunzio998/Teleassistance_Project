import os
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.metrics import silhouette_samples
import numpy as np
from sklearn.preprocessing import LabelEncoder
import logging

# Configuro il logger
logging.basicConfig(level=logging.INFO,  # Imposto il livello minimo di log
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Formato del log


def compute_silhouette_score(df: pd.DataFrame):
    """
    Calcola e restituisce il Silhouette Score per il clustering effettuato,
    normalizzato nel range [0, 1].
    :param df: DataFrame con i dati (inclusa la colonna 'Cluster')
    :return: Valore normalizzato del Silhouette Score
    """

    # Rimuoviamo la colonna Cluster dalle feature per il calcolo del silhouette
    encoded_features = df.drop(columns=['Cluster'])
    labels = df['Cluster']

    # Convertiamo le colonne categoriali in numeriche utilizzando LabelEncoder
    for col in encoded_features.columns:
        if encoded_features[col].dtype == 'object' or encoded_features[col].dtype.name == 'category':
            le = LabelEncoder()
            encoded_features[col] = le.fit_transform(encoded_features[col])

    # Calcola i valori del silhouette per ciascun campione
    silhouette_values = silhouette_samples(encoded_features, labels)

    # Normalizza i valori del silhouette nel range [0, 1]
    normalized_silhouette_values = (silhouette_values - silhouette_values.min()) / (
                silhouette_values.max() - silhouette_values.min())

    # Calcola il silhouette score medio normalizzato
    final_score = np.mean(normalized_silhouette_values)

    print(f"L'indice di Silhouette medio normalizzato è : {final_score}")

    return final_score


def compute_purity(df: pd.DataFrame, target_column: str):
    """
    Calcola la purezza del clustering per ciascun cluster e la purezza media ponderata.

    :param df: DataFrame contenente i dati con le colonne 'Cluster' e la colonna target.
    :param target_column: Colonna target rispetto alla quale calcolare la purezza.
    :return: Dizionario con le purezze per ciascun cluster e purezza complessiva.
    """
    cluster_purity = {}
    total_samples = len(df)

    for cluster in df['Cluster'].unique():
        cluster_data = df[df['Cluster'] == cluster]

        # Trova la classe più comune nel cluster
        most_common_class = cluster_data[target_column].mode()[0]

        # Calcola la purezza del cluster
        purity = (cluster_data[target_column] == most_common_class).sum() / len(cluster_data)
        cluster_purity[cluster] = purity

    # Calcolo purezza media ponderata
    weighted_purity = sum(purity * len(df[df['Cluster'] == cluster]) for cluster, purity in cluster_purity.items())
    purity_score = weighted_purity / total_samples

    # Stampa purezza di ciascun cluster con più decimali
    logging.info("Purezza di ciascun cluster:")
    for cluster, purity in cluster_purity.items():
        logging.info(f"Cluster {cluster}: Purezza = {purity:.5f}")

    # Stampa purezza complessiva con più decimali
    logging.info(f"Purezza complessiva: {purity_score:.5f}\n")

    return cluster_purity, purity_score


def plot_purity_bars(cluster_purity, overall_purity):
    """
    Crea un grafico a barre per visualizzare la purezza di ciascun cluster e la purezza complessiva.
    :param cluster_purity: Dizionario con la purezza di ciascun cluster.
    :param overall_purity: Purezza media complessiva.
    """
    if not os.path.exists('graphs'):
        os.makedirs('graphs')

    clusters = list(cluster_purity.keys())
    purity_values = list(cluster_purity.values())

    plt.figure(figsize=(10, 6))
    plt.bar(clusters, purity_values, color='skyblue', label='Purezza Cluster')
    plt.axhline(y=overall_purity, color='r', linestyle='--', label='Purezza Complessiva')

    plt.xlabel('Cluster')
    plt.ylabel('Purezza')
    plt.title('Purezza dei Cluster')
    plt.legend()

    # Salva il grafico
    plt.savefig('graphs/cluster_purity.png')
    plt.close()


def compute_final_metric(purity_score: float, silhouette_score: float, num_clusters: int) -> float:
    """
    Calcola la metrica finale, come la media delle due metriche normalizzate
    e sottraendo un termine di penalità pari a 0,05 volte il numero di cluster.
    :param purity_score: La purezza normalizzata.
    :param silhouette_score: Il punteggio di silhouette normalizzato.
    :param num_clusters: Numero di cluster utilizzati nel clustering.
    :return: La metrica finale calcolata.
    """

    # Calcola la media delle due metriche normalizzate
    mean_normalized_metric = (purity_score + silhouette_score) / 2

    # Calcola il termine di penalità
    penalty = 0.05 * num_clusters

    # Calcola la metrica finale sottraendo la penalità
    final_metric = mean_normalized_metric - penalty

    return final_metric


def compute_all_metrics(df: pd.DataFrame, label_encoders, target_column='incremento'):
    """
     Calcola tutte le metriche e genera i grafici per il clustering.
     :param df: DataFrame contenente i dati, inclusi i cluster.
     :param label_encoders: Dizionario di LabelEncoders per la decodifica.
     :param target_column: La colonna target rispetto alla quale calcolare le metriche.
     """

    # Calcolo la purezza di ogni cluster e la purezza media del clustering
    cluster_purity, purity_score = compute_purity(df, target_column)

    # Calcolo dell'indice di Silhouette
    #silhouette_score = compute_silhouette_score(df)

    # Plot della purezza dei cluster
    plot_purity_bars(cluster_purity, purity_score)

    # Calcolo della metrica finale
    #final_metric = compute_final_metric(purity_score,silhouette_score, num_clusters=len(df['Cluster'].unique()))
    #print(f"\nLa metrica finale è : {final_metric:.2f}")



