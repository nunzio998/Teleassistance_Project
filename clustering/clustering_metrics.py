import os
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


def compute_purity(df, target_column):
    """
    Calcola la purezza del clustering per ciascun cluster e la purezza media ponderata.

    :param data: DataFrame contenente i dati con le colonne 'Cluster' e la colonna target.
    :param target_column: Colonna target rispetto alla quale calcolare la purezza.
    :return: Dizionario con le purezze per ciascun cluster e purezza complessiva.
    """
    cluster_purity = {}
    for cluster in df['Cluster'].unique():
        cluster_data = df[df['Cluster'] == cluster]

        # Trova la classe più comune nel cluster
        most_common_class = cluster_data[target_column].mode()[0]

        # Calcola la purezza del cluster
        purity = (cluster_data[target_column] == most_common_class).sum() / len(cluster_data)
        cluster_purity[cluster] = purity

    # Stampa la purezza di ciascun cluster
    print("Purezza di ciascun cluster:")
    for cluster, purity in cluster_purity.items():
        print(f"Cluster {cluster}: Purezza = {purity:.2f}")

    # Calcola la purezza media ponderata
    cluster_sizes = df['Cluster'].value_counts()
    total_purity = sum(cluster_purity[cluster] * cluster_sizes[cluster] for cluster in cluster_purity)
    purity_score = total_purity / len(df)

    print(f"\nPurezza complessiva del clustering: {purity_score:.2f}")

    return cluster_purity, purity_score


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


def plot_increment_distribution(df):
    """
    Crea un grafico a barre impilate per la distribuzione della variabile 'incremento' per ciascun cluster.
    :param df: DataFrame contenente i dati con le colonne 'Cluster' e 'incremento'.
    """
    # Crea la cartella 'graphs' se non esiste
    if not os.path.exists('graphs'):
        os.makedirs('graphs')

    plt.figure(figsize=(12, 6))

    # Crea il grafico con seaborn
    ax = sns.countplot(data=df, x='Cluster', hue='incremento')
    # Aggiungi la legenda esplicitamente
    handles, labels = ax.get_legend_handles_labels()
    if not labels:
        labels = ['Incremento {}'.format(i) for i in df['incremento'].unique()]

    ax.legend(handles=handles, labels=labels, title='Incremento')
    plt.title('Distribuzione della variabile target Incremento per Cluster')
    plt.xlabel('Cluster')
    plt.ylabel('Numero di Campioni')

    # Salva il grafico nella cartella 'graphs'
    plt.savefig('graphs/plot_increment_distribution.png')
    plt.close()


def compute_all_metrics(df, encoded_features, labels):

    # Plotto la distribuzione dell'incremento rispetto ad ogni cluster
    plot_increment_distribution(df)

    # calcolo la purezza di ogni cluster e la purezza media del clustering
    print("\nCalcolo della purezza di ciascun cluster e della purezza complessiva:")
    cluster_purity, purity_score = compute_purity(df, 'incremento')

    print("\nCalcolo dell'indice di Silhouette... Attendi...")
    silhouette_score = compute_silhouette_score(encoded_features,labels)

    print("\nCalcolo la metrica finale...")
    final_metric = compute_final_metric(purity_score, silhouette_score, num_clusters=4)
    print("\nLa metrica finale è : ", final_metric)

    return final_metric


