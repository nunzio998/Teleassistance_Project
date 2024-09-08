import os

from matplotlib import pyplot as plt
import seaborn as sns


def silhoutte_index():
    return None

def compute_purity():
    return None


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