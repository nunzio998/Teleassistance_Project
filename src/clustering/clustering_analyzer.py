import logging
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

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
    # Generazione di grafici per caratteristiche numeriche
    plot_year_month_features(df,cluster_year_mapping)
    plot_age_distribution_by_age_group(df, cluster_year_mapping)
    plot_duration_distribution_by_duration_group(df, numerical_features)

    # Generazione di grafici per feature categoriche
    plot_categorical_features(df, categorical_features, reverse_mapping, cluster_year_mapping)


def plot_year_month_features(df, cluster_year_mapping):
    """
    Crea grafici a barre solo per le feature 'year' e 'month', ordinando i cluster in base agli anni e mesi,
    con miglioramenti per la leggibilità e la precisione.
    """

    def sort_key(cluster):
        years = cluster_year_mapping[cluster].split(', ')
        if len(years) == 1:
            return int(years[0].split()[0]), 0
        else:
            return int(years[0].split()[0]), 1

    ordered_clusters = sorted(cluster_year_mapping.keys(), key=sort_key)

    # Considera solo 'year' e 'month'
    numerical_features = ['year', 'month']

    for feature in numerical_features:
        plt.figure(figsize=(10, 6))

        sns.set(style="whitegrid")

        # Calcola il conteggio delle occorrenze di ciascun valore per la feature corrente
        count_data = df.groupby([feature, 'Cluster']).size().reset_index(name='counts')

        # Crea il bar plot
        sns.barplot(x=feature, y='counts', hue='Cluster', data=count_data, palette="Set2")

        # Miglioramento della leggibilità con titoli ed etichette
        plt.title(f'Distribuzione per {feature} per Cluster (ordinati per anni e mesi)', fontsize=18)
        plt.xlabel(feature.capitalize(), fontsize=14)
        plt.ylabel('Count', fontsize=14)
        plt.xticks(fontsize=12)
        plt.yticks(fontsize=12)

        # Aggiungi una griglia
        plt.grid(True, which='both', axis='y', linestyle='--', linewidth=0.5)

        # Linee tratteggiate rosse per distinguere tutti i mesi
        for i in range(1, len(count_data[feature].unique())):
            plt.axvline(i - 0.5, color='red', linestyle='--', linewidth=1.5)

        # Aggiungi una nota sugli anni sotto il grafico
        year_annotation = "\n".join(
            [f"Cluster {cluster}: Anni {cluster_year_mapping[cluster]}" for cluster in ordered_clusters])
        plt.annotate(year_annotation, xy=(0.5, -0.2), xycoords="axes fraction", fontsize=12, ha="center", va="center")

        # Salva il grafico
        plt.savefig(f'graphs/{feature}_by_cluster.png', bbox_inches='tight')
        plt.close()


def plot_age_distribution_by_age_group(df, cluster_year_mapping):
    """
    Crea un grafico a barre per la distribuzione dell'età del paziente, suddividendo l'età in fasce specifiche,
    e mostra la distribuzione per cluster.
    """

    # Definisci le fasce d'età
    bins = [0, 15, 25, 35, 45, 60, 71, 100]
    labels = ['0-15', '16-25', '26-35', '36-45', '46-60', '61-71', '82-100']

    # Crea una nuova colonna nel dataframe che classifica le età nelle fasce
    df['fasce_eta'] = pd.cut(df['eta_paziente'], bins=bins, labels=labels, right=False)

    # Calcola il conteggio delle occorrenze per ciascuna fascia d'età e cluster
    count_data = df.groupby(['fasce_eta', 'Cluster'], observed=False).size().reset_index(name='counts')

    # Crea il bar plot
    plt.figure(figsize=(10, 6))
    sns.set(style="whitegrid")
    sns.barplot(x='fasce_eta', y='counts', hue='Cluster', data=count_data, palette="Set2")

    # Miglioramento della leggibilità con titoli ed etichette
    plt.title('Distribuzione per Fasce di Età per Cluster', fontsize=18)
    plt.xlabel('Fasce di Età', fontsize=14)
    plt.ylabel('Count', fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    # Aggiungi una griglia
    plt.grid(True, which='both', axis='y', linestyle='--', linewidth=0.5)

    # Linee tratteggiate rosse per separare le fasce d'età
    for i in range(1, len(labels)):
        plt.axvline(i - 0.5, color='red', linestyle='--', linewidth=1.5)

    # Aggiungi una nota sugli anni sotto il grafico
    year_annotation = "\n".join(
        [f"Cluster {cluster}: Anni {cluster_year_mapping[cluster]}" for cluster in cluster_year_mapping.keys()])
    plt.annotate(year_annotation, xy=(0.5, -0.2), xycoords="axes fraction", fontsize=12, ha="center", va="center")

    # Salva il grafico
    plt.savefig(f'graphs/eta_paziente_by_cluster.png', bbox_inches='tight')
    plt.close()


def plot_duration_distribution_by_duration_group(df, numerical_features):
    """
    Crea un grafico a barre per la distribuzione della durata della televisita, suddividendo la durata in fasce specifiche,
    e mostra la distribuzione per cluster.
    """

    if 'durata_televisita' in numerical_features:
        # Definisci le fasce di durata (in minuti)
        bins = [0, 10, 20, 30, 45, 60, np.inf]
        labels = ['0-10 min', '11-20 min', '21-30 min', '31-45 min', '46-60 min', '>60 min']

        # Crea una nuova colonna nel dataframe che classifica le durate nelle fasce
        df['fasce_durata'] = pd.cut(df['durata_televisita'], bins=bins, labels=labels, right=False)

        # Calcola il conteggio delle occorrenze per ciascuna fascia di durata e cluster
        count_data = df.groupby(['fasce_durata', 'Cluster'], observed=False).size().reset_index(name='counts')

        # Crea il bar plot
        plt.figure(figsize=(10, 6))
        sns.set(style="whitegrid")
        sns.barplot(x='fasce_durata', y='counts', hue='Cluster', data=count_data, palette="Set2")

        # Miglioramento della leggibilità con titoli ed etichette
        plt.title('Distribuzione per Fasce di Durata Televisita per Cluster', fontsize=18)
        plt.xlabel('Fasce di Durata (min)', fontsize=14)
        plt.ylabel('Count', fontsize=14)
        plt.xticks(fontsize=12)
        plt.yticks(fontsize=12)

        # Aggiungi una griglia
        plt.grid(True, which='both', axis='y', linestyle='--', linewidth=0.5)

        # Linee tratteggiate rosse per separare le fasce di durata
        for i in range(1, len(labels)):
            plt.axvline(i - 0.5, color='red', linestyle='--', linewidth=1.5)

        # Salva il grafico
        plt.savefig(f'graphs/durata_televisita_by_cluster.png', bbox_inches='tight')
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
