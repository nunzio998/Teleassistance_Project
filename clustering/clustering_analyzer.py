import os
import matplotlib.pyplot as plt
import seaborn as sns

def plot_numerical_features(df, numerical_features):
    """
       Crea boxplot per ogni feature numerica specificata e li salva in una cartella chiamata 'graphs'.

       Args:
           df: DataFrame contenente i dati di clustering.
           numerical_features: Lista di feature numeriche che si desidera visualizzare.

       Returns:
           None: I grafici vengono salvati come file PNG.
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
    :param reverse_mapping: Dizionario che mappa i numeri alle categorie originali
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


def analyze_clustering(df, numerical_features, categorical_features,reverse_mapping):

    # Generazione di grafici per caratteristiche numeriche
    print("Generazione di grafici per caratteristiche numeriche...")
    plot_numerical_features(df, numerical_features)

    # Generazione di grafici per caratteristiche categoriali
    print("Generazione di grafici per caratteristiche categoriali...")
    plot_categorical_features(df, categorical_features,reverse_mapping)

    return


"""
Considerazioni:
1. Durata televisita infermiere: se è inferiore a un tot, vuol dire che non aveva bisogno di infermiere
 e lo ha reindirizzato?

"""