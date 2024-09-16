import os
import logging
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mticker

# Configuro il logger
logging.basicConfig(level=logging.INFO,  # Imposto il livello minimo di log
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Formato del log


def analyze_clustering(df, numerical_features, categorical_features, reverse_mapping, cluster_year_mapping):
    """
    Genera grafici per analizzare la distribuzione delle feature nei cluster.
    :param df: dataFrame
    :param numerical_features: feature numeriche
    :param categorical_features: feature categoriche
    :param reverse_mapping: dizionario che mappa la feature al loro valore codificato
    :param cluster_year_mapping: anni per ogni cluster salvati in un dizionario
    :return: None
    """
    # Generazione di grafici per feature numeriche
    plot_numerical_features(df, numerical_features, cluster_year_mapping)

    # Generazione di grafici per feature categoriche
    plot_categorical_features(df, categorical_features, reverse_mapping, cluster_year_mapping)


def plot_numerical_features(df, numerical_features, cluster_year_mapping):
    """
    Crea grafici boxplot per le feature numeriche, ordinando i cluster in base agli anni e mesi,
    con miglioramenti per la leggibilità e la precisione.
    """

    def sort_key(cluster):
        years = cluster_year_mapping[cluster].split(', ')
        if len(years) == 1:
            return int(years[0].split()[0]), 0
        else:
            return int(years[0].split()[0]), 1

    ordered_clusters = sorted(cluster_year_mapping.keys(), key=sort_key)

    for feature in numerical_features:
        plt.figure(figsize=(10, 6))

        sns.set(style="whitegrid")
        sns.boxplot(x='Cluster', y=feature, data=df, order=ordered_clusters,
                    showmeans=True, meanline=True,
                    meanprops={'color': 'red', 'linestyle': '--', 'linewidth': 2},
                    boxprops={'facecolor': 'lightblue', 'edgecolor': 'darkblue', 'linewidth': 2},
                    whiskerprops={'linewidth': 2, 'color': 'darkblue'},
                    capprops={'linewidth': 2, 'color': 'darkblue'},
                    medianprops={'linewidth': 2, 'color': 'green'})

        # Definisci il range dei tick sull'asse Y (più dettagliato)
        y_min, y_max = df[feature].min(), df[feature].max()

        if 'year' in feature.lower() or 'month' in feature.lower():  # Controlla se è una variabile temporale
            # Tick per anni o mesi (passo 1)
            plt.yticks(np.arange(y_min, y_max + 1, 1))
            plt.gca().yaxis.set_major_locator(mticker.MultipleLocator(1))
        else:
            # Tick per altre variabili (passo 5)
            plt.yticks(np.arange(y_min, y_max + 1, 5))
            plt.gca().yaxis.set_major_locator(mticker.MultipleLocator(5))
            plt.gca().yaxis.set_minor_locator(mticker.MultipleLocator(1))

        plt.grid(True, which='both', axis='y', linestyle='--', linewidth=0.5)

        # Miglioramento della leggibilità con titoli ed etichette
        plt.title(f'Distribuzione per {feature} per Cluster (ordinati per anni e mesi)', fontsize=18)
        plt.xlabel('Cluster', fontsize=14)
        plt.ylabel(feature, fontsize=14)
        plt.xticks(fontsize=12)
        plt.yticks(fontsize=12)

        # Linee tratteggiate rosse per distinguere i cluster
        for i in range(1, len(ordered_clusters)):
            plt.axvline(i - 0.5, color='red', linestyle='--', linewidth=1.5)

        # Aggiungi una nota sugli anni sotto il grafico
        year_annotation = "\n".join(
            [f"Cluster {cluster}: Anni {cluster_year_mapping[cluster]}" for cluster in ordered_clusters])
        plt.annotate(year_annotation, xy=(0.5, -0.2), xycoords="axes fraction", fontsize=12, ha="center", va="center")

        # Salva il grafico
        plt.savefig(f'graphs/{feature}_by_cluster.png', bbox_inches='tight')
        plt.close()


def plot_categorical_features(df, categorical_features, reverse_mapping, cluster_year_mapping):
    """
    Crea un grafico countplot per le feature categoriche e utilizza il reverse_mapping per associare i numeri alle categorie originali,
    ordinando le categorie in base al numero di occorrenze per ogni cluster e includendo anni e mesi.
    :param df: DataFrame con i dati
    :param categorical_features: Lista delle feature categoriche
    :param reverse_mapping: Dizionario che mappa i numeri alle categorie originali
    :param cluster_year_mapping: Dizionario che mappa ogni cluster agli anni e mesi corrispondenti
    """

    # Funzione per ordinare i cluster mettendo prima quelli con anni singoli e poi quelli con range di anni.
    def sort_key(cluster):
        years = cluster_year_mapping[cluster].split(', ')
        if len(years) == 1:  # Cluster con un solo anno
            return int(years[0].split()[0]), 0
        else:  # Cluster con un intervallo di anni
            return int(years[0].split()[0]), 1

    # Ordina i cluster in base al criterio degli anni
    ordered_clusters = sorted(cluster_year_mapping.keys(), key=sort_key)

    for feature in categorical_features:
        plt.figure(figsize=(12, 7))

        # Otteniamo l'ordine delle categorie basato sul numero di occorrenze per cluster
        order_by_cluster = (
            df.groupby(['Cluster', feature]).size().reset_index(name='counts')
            .sort_values(by=['Cluster', 'counts'], ascending=[True, False])
        )

        # Estrai l'ordine corretto delle categorie per hue_order
        ordered_categories = order_by_cluster[feature].unique()

        sns.set(style="whitegrid")
        sns.countplot(x='Cluster', hue=feature, data=df, palette='Set1', hue_order=ordered_categories,
                      order=ordered_clusters)

        # Migliora la leggibilità del grafico
        plt.title(f'Distribuzione per {feature} per Cluster (ordinati per anni e mesi)', fontsize=18)
        plt.xlabel('Cluster', fontsize=14)
        plt.ylabel('Count', fontsize=14)
        plt.xticks(fontsize=12)
        plt.yticks(fontsize=12)

        # Aggiungi linee tratteggiate rosse per separare i cluster
        for i in range(1, len(ordered_clusters)):
            plt.axvline(i - 0.5, color='red', linestyle='--')

        # Aggiungi una nota sugli anni e mesi sotto il grafico
        year_annotation = "\n".join(
            [f"Cluster {cluster}: Anni e mesi {cluster_year_mapping[cluster]}" for cluster in ordered_clusters])
        plt.annotate(year_annotation, xy=(0.5, -0.2), xycoords="axes fraction", fontsize=12, ha="center", va="center")

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
            plt.legend(handles=handles, labels=new_labels, title=feature, bbox_to_anchor=(1.05, 1), loc='upper left',
                       fontsize=12, title_fontsize=14)
        else:
            # Se non esiste una mappatura inversa, utilizza le etichette originali
            plt.legend(title=feature, bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12, title_fontsize=14)

        # Salva il grafico senza taglio laterale
        file_path = f'graphs/{feature}_by_cluster.png'
        plt.savefig(file_path, bbox_inches='tight')
        plt.close()
