from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import Pipeline
from clustering.clustering_analyzer import analyze_clustering
from clustering.clustering_metrics import compute_all_metrics

def execute_clustering(df, label_encoders, numerical_features, categorical_features, reverse_mapping):
    """
    Metodo che esegue tutti i metodi del file clustering_execution
    :param df: dataFrame
    :return: df
    """
    # Calcolo del numero ottimale di cluster
    plot_elbow_method(df, max_clusters=10)

    # Applicazione del clustering
    labels, svd_data = apply_clustering(df, n_clusters=4)

    # Aggiungiamo le etichette e le componenti principali al dataframe originale
    df['Cluster'] = labels

    # Genera il dizionario automaticamente
    cluster_year_mapping = generate_cluster_year_mapping(df, year_column='year')

    # Generazione dei plot per analizzare il clustering
    analyze_clustering(df, numerical_features, categorical_features, reverse_mapping, cluster_year_mapping)

    # Calcolo delle metriche
    compute_all_metrics(df, target_column='incremento', label_encoders=label_encoders)

    return df, labels, svd_data

def plot_elbow_method(data, max_clusters=10):
    """
    Visualizza l'elbow method per la ricerca del numero ottimale di cluster.
    :param data: dati del clustering
    :param max_clusters: numero massimo di cluster da esplorare
    :return: None
    """
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


def apply_clustering(data, n_clusters=4, n_components=None):
    """
    Esegue il clustering K-Means applicando una riduzione della dimensionalità dei dati con TruncatedSVD.
    :param data: dataFrame dei dati
    :param n_clusters: numero di cluster
    :param n_components:
    :return labels, svd_data: etichette del clusterin e dati trasformati con Truncated SVD
    """

    if n_components is None:
        n_components = min(10, data.shape[1])  # Imposta il numero massimo di componenti in base al numero di feature

    # Pipeline per il clustering con TruncatedSVD
    pipeline = Pipeline(steps=[
        ('dim_reduction', TruncatedSVD(n_components=n_components)),
        ('clustering', KMeans(n_clusters=n_clusters, random_state=42))
    ])
    labels = pipeline.fit_predict(data)
    svd_data = pipeline.named_steps['dim_reduction'].transform(data)
    return labels, svd_data

def generate_cluster_year_mapping(df, year_column='year', month_column='month'):
    """
    Genera automaticamente un dizionario che mappa i cluster agli anni e mesi corrispondenti.
    :param df: DataFrame con i dati, inclusi i cluster e le colonne degli anni e dei mesi.
    :param year_column: Nome della colonna che contiene le informazioni sugli anni.
    :param month_column: Nome della colonna che contiene le informazioni sui mesi.
    :return: Dizionario che mappa i cluster agli anni e mesi corrispondenti.
    """
    # Raggruppa i dati per Cluster, Anno e Mese, e conta le occorrenze
    year_month_distribution = df.groupby(['Cluster', year_column, month_column]).size().reset_index(name='counts')

    # Crea un dizionario vuoto per la mappatura
    cluster_year_mapping = {}

    # Per ogni cluster, crea una stringa che rappresenta gli anni e i mesi associati
    for cluster in year_month_distribution['Cluster'].unique():
        cluster_data = year_month_distribution[year_month_distribution['Cluster'] == cluster]

        # Crea un dizionario temporaneo per memorizzare i mesi per ogni anno
        years_to_months = {}

        for _, row in cluster_data.iterrows():
            year = row[year_column]
            month = row[month_column]

            if year not in years_to_months:
                years_to_months[year] = []
            years_to_months[year].append(month)

        # Crea una stringa che rappresenta l'associazione di anni e mesi
        year_month_strings = []
        for year, months in years_to_months.items():
            month_range = f"{min(months)}-{max(months)}" if len(months) > 1 else str(min(months))
            year_month_strings.append(f"{year} (mesi {month_range})")

        # Unisci le stringhe di anno e mese
        year_string = ", ".join(year_month_strings)
        cluster_year_mapping[cluster] = year_string

    return cluster_year_mapping


