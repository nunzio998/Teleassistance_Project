import os
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from sklearn.metrics import silhouette_samples
import numpy as np


def compute_silhouette_score(encoded_features, labels):
    """
    Calcola e restituisce il Silhouette Score per il clustering effettuato,
    normalizzato nel range [0, 1].

    :param encoded_features: Array di feature codificate (input del clustering)
    :param labels: Etichette dei cluster generate dal modello K-Means
    :return: Valore normalizzato del Silhouette Score
    """
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

    :param data: DataFrame contenente i dati con le colonne 'Cluster' e la colonna target.
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

    print("Purezza di ciascun cluster:")
    for cluster, purity in cluster_purity.items():
        print(f"Cluster {cluster}: Purezza = {purity:.2f}")
    print(f"Purezza complessiva: {purity_score:.2f}\n")

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


def plot_increment_distribution(df: pd.DataFrame, label_encoders: dict):
    """
    Crea un grafico a barre impilate per la distribuzione della variabile 'incremento' per ciascun cluster.
    :param df: DataFrame contenente i dati con le colonne 'Cluster' e 'incremento'.
    """
    if 'incremento' in label_encoders:
        le_incremento = label_encoders['incremento']
        df['incremento'] = le_incremento.inverse_transform(df['incremento'])

    # Crea la cartella 'graphs' se non esiste
    if not os.path.exists('graphs'):
        os.makedirs('graphs')

    plt.figure(figsize=(12, 6))
    sns.countplot(data=df, x='Cluster', hue='incremento')
    plt.title('Distribuzione della variabile target Incremento per Cluster')
    plt.xlabel('Cluster')
    plt.ylabel('Numero di Campioni')
    plt.legend(title='Incremento')

    # Salva il grafico nella cartella 'graphs'
    plt.savefig('graphs/plot_increment_distribution.png')
    plt.close()

def plot_pca_components(pca_data, labels):
    """
    Crea un grafico delle prime due componenti principali per visualizzare la distribuzione dei cluster.
    :param pca_data: I dati ridotti dimensionalmente con PCA.
    :param labels: Etichette dei cluster.
    """
    if not os.path.exists('graphs'):
        os.makedirs('graphs')

    plt.figure(figsize=(10, 6))
    plt.scatter(pca_data[:, 0], pca_data[:, 1], c=labels, cmap='viridis', marker='o')
    plt.title('Distribuzione delle Componenti PCA per Cluster')
    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.colorbar(label='Cluster')

    # Salva il grafico
    plt.savefig('graphs/pca_cluster_distribution.png')
    plt.close()

def compute_all_metrics(df: pd.DataFrame, label_encoders, target_column='incremento', pca_data=None):
    """
     Calcola tutte le metriche e genera i grafici per il clustering.
     :param df: DataFrame contenente i dati, inclusi i cluster.
     :param label_encoders: Dizionario di LabelEncoders per la decodifica.
     :param target_column: La colonna target rispetto alla quale calcolare le metriche.
     :param pca_data: I dati ridotti dimensionalmente con PCA (opzionale).
     """

    # Plotto la distribuzione dell'incremento rispetto ad ogni cluster
    plot_increment_distribution(df, label_encoders)

    # calcolo la purezza di ogni cluster e la purezza media del clustering
    print("\nCalcolo della purezza di ciascun cluster e della purezza complessiva:")
    cluster_purity, purity_score = compute_purity(df, target_column)

    print("\nCalcolo dell'indice di Silhouette... Attendi...")
    #silhouette_score = compute_silhouette_score(encoded_features,labels)

    # Plot della purezza dei cluster
    plot_purity_bars(cluster_purity, purity_score)

    # Visualizzazione delle componenti PCA se disponibili
    if pca_data is not None:
        plot_pca_components(pca_data, df['Cluster'])

    #print("\nCalcolo la metrica finale...")
    #final_metric = compute_final_metric(purity_score, silhouette_score, num_clusters=4)
    #print("\nLa metrica finale è : ", final_metric)



