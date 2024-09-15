import os
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# Configuro il logger
logging.basicConfig(level=logging.INFO,  # Imposto il livello minimo di log
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Formato del log

def analyze_clustering(df, numerical_features, categorical_features,reverse_mapping):
    """
    Genera grafici per analizzare la distribuzione delle feature nei cluster.
    :param df: dataFrame
    :param numerical_features: feature numeriche
    :param categorical_features: feature categoriche
    :param reverse_mapping: dizionario che mappa la feature al loro valore codificato
    :return: None
    """
    # Generazione di grafici per caratteristiche numeriche
    logging.info("Generazione di grafici per caratteristiche numeriche...")
    plot_numerical_features(df, numerical_features)

    # Generazione di grafici per caratteristiche categoriali
    logging.info("Generazione di grafici per caratteristiche categoriali...")
    plot_categorical_features(df, categorical_features,reverse_mapping)

    return

def plot_numerical_features(df, numerical_features):
    """
       Crea boxplot per ogni feature numerica specificata e li salva in una cartella chiamata 'graphs'.
        :param df: DataFrame contenente i dati di clustering.
        :param numerical_features: Lista di feature numeriche che si desidera visualizzare.
        :return None
       """
    for feature in numerical_features:
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='Cluster', y=feature, data=df)
        plt.title(f'Distribuzione di {feature} per Cluster')
        plt.savefig(f'graphs/{feature}_by_cluster.png')
        plt.close()


def plot_categorical_features(df, categorical_features, reverse_mapping):
    """
    Crea un grafico countplot per le feature categoriche e utilizza il reverse_mapping per associare i numeri alle categorie originali.
    :param df: DataFrame con i dati
    :param categorical_features: Lista delle feature categoriche
    :param reverse_mapping: dizionario che mappa la feature al loro valore codificato
    """
    # Creare la cartella 'graphs' se non esiste
    os.makedirs('graphs', exist_ok=True)

    for feature in categorical_features:
        plt.figure(figsize=(12, 7))
        sns.countplot(x='Cluster', hue=feature, data=df, palette='Set2')
        plt.title(f'Distribuzione di {feature} per Cluster', fontsize=16)
        plt.xlabel('Cluster', fontsize=14)
        plt.ylabel('Count', fontsize=14)
        plt.xticks(fontsize=12)
        plt.yticks(fontsize=12)

        # Ottieni la mappatura inversa per la feature corrente (se disponibile)
        if feature in reverse_mapping:
            handles, labels = plt.gca().get_legend_handles_labels()
            new_labels = []
            for label in labels:
                try:
                    # Cerca di convertire l'etichetta in un numero e usare la mappatura
                    num_label = int(label)
                    new_label = reverse_mapping[feature].get(num_label, label)
                except ValueError:
                    # Se non è un numero, lascia l'etichetta invariata
                    new_label = label
                new_labels.append(new_label)

            # Imposta la nuova legenda con le etichette mappate
            plt.legend(handles=handles, labels=new_labels, title=feature, bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12, title_fontsize=14)
        else:
            # Se non esiste una mappatura inversa, utilizza le etichette originali
            plt.legend(title=feature, bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12, title_fontsize=14)

        # Salvare il grafico senza taglio laterale
        file_path = f'graphs/{feature}_by_cluster.png'
        plt.savefig(file_path, bbox_inches='tight')
        plt.close()






"""
Considerazioni:
1. Durata televisita infermiere: se è inferiore a un tot, vuol dire che non aveva bisogno di infermiere
 e lo ha reindirizzato?
"""