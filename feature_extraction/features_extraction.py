from datetime import datetime
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

def extract_durata_televisita(df):
    """
    Calcola la durata della televisita.
    :param df: dataFrame
    :return: dataFrame che associa ad ogni campione la durata della televisita
     """
    # Assicurarsi che 'ora_inizio_erogazione' e 'ora_fine_erogazione' siano in formato datetime
    df['ora_inizio_erogazione'] = pd.to_datetime(df['ora_inizio_erogazione'], errors='coerce')
    df['ora_fine_erogazione'] = pd.to_datetime(df['ora_fine_erogazione'], errors='coerce')
    def calcola_durata(row):
        """
        Calcola la durata della televisita in secondi tra 'ora_inizio_erogazione' e 'ora_fine_erogazione'.
        :param row: una riga del DataFrame
        :return: durata in secondi
        """
        if pd.notnull(row['ora_inizio_erogazione']) and pd.notnull(row['ora_fine_erogazione']):
            durata = row['ora_fine_erogazione'] - row['ora_inizio_erogazione']
            return int(durata.total_seconds()/60)
        else:
            return None

    df['durata_televisita'] = df.apply(calcola_durata, axis=1)

    return df


def remove_ora_erogazione(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove le feature 'ora_inizio_erogazione' e 'ora_fine_erogazione' dal dataFrame.
    :param df: dataFrame
    :return df: dataFrame senza le colonne specificate.
    """
    df.drop(columns=['ora_inizio_erogazione', 'ora_fine_erogazione'], inplace=True)
    return df


def extract_eta_paziente(df):
    """
    Estrae l'età del paziente.
    :param df: dataFrame
    :return df: dataFrame con la colonna 'età'
    """
    # Assicurarsi che 'data_nascita' sia in formato datetime
    df['data_nascita'] = pd.to_datetime(df['data_nascita'], errors='coerce')

    # Calcolare l'età in anni
    current_date = datetime.now()
    def calcola_eta(row):
        """
        Calcola l'età.
        :param row: una riga del DataFrame.
        :return age: età del paziente.
        """
        birth_date = row['data_nascita']
        if pd.isnull(birth_date):
            return None
        age = current_date.year - birth_date.year - (
                (current_date.month, current_date.day) < (birth_date.month, birth_date.day))
        return age

    df['eta_paziente'] = df.apply(calcola_eta, axis=1)

    return df

def remove_data_nascita(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove la feature 'data_nascita' dal dataframe.
    :param df: dataFrame
    :return df: dataFrame senza la colonna 'data_nascita'
    """
    df.drop(columns=['data_nascita'], inplace=True)
    return df


def extract_year_and_month(df):
    """
    Estrae l'anno e il mese dalla colonna 'data_erogazione' e crea le nuove colonne 'year' e 'month' nel DataFrame.
    :param df: dataFrame originale contenente la colonna 'data_erogazione'
    :return df: dataFrame originale con le nuove colonne 'year' e 'month'
    """
    # Assicurati che la colonna 'data_erogazione' sia nel formato datetime
    df['data_erogazione'] = pd.to_datetime(df['data_erogazione'], errors='coerce')

    # Crea nuove colonne 'year' e 'month' estraendo l'anno e il mese dalla colonna 'data_erogazione'
    df['year'] = df['data_erogazione'].dt.year
    df['month'] = df['data_erogazione'].dt.month

    return df

def save_grouped_by_year_and_month(df, directory='month_dataset'):

    """
    Raggruppa il DataFrame per anno e mese e salva ogni gruppo in un file Parquet separato
    nella directory 'month_dataset'.
    :param df: dataFrame originale contenente la colonna 'data_erogazione'
    :return df: dataFrame con le nuove colonne 'year' e 'month'
    """
    # Crea la directory se non esiste
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory '{directory}' creata.")

    # Raggruppa il DataFrame per anno e mese e salva ogni gruppo in un file Parquet separato
    for (year, month), group in df.groupby(['year', 'month']):
        output_path = f'month_dataset/Anno_{year}_Mese_{month}.parquet'

        # Se il file esiste, salta la creazione
        if not os.path.isfile(output_path):
            group.to_parquet(output_path, index=False)

    for file_name in os.listdir(directory):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(directory, file_name)
            df = pd.read_parquet(file_path)
    return df

def conta_professionisti_per_mese(cartella):

    """
    Conta per ogni mese il numero di volte in cui compare ogni professionista sanitario.
    :param cartella (str): percorso della cartella contenente i file Parquet divisi per mese e anno.
    :return df_aggregato: dataFrame contenente il numero di occorrenze per ogni tipologia di professionista
                          sanitario per ogni mese.
    """
    dati_aggregati = []
    def conta_occorrenze_professionisti(df, colonna='tipologia_professionista_sanitario'):
        """
        Conta il numero di occorrenze di ciascuna tipologia di professionista sanitario in un DataFrame.
        :param df: dataFrame
        :param colonna: colonna relativa alla tipologia di professionista sanitario
        :return: conteggio delle occorrenze per ogni tipologia di professionista sanitario
        """
        return df[colonna].value_counts()

    # Scorri tutti i file Parquet nella cartella
    for file in os.listdir(cartella):
        if file.endswith(".parquet"):
            percorso_file = os.path.join(cartella, file)

            # Leggi il file Parquet in un DataFrame
            df = pd.read_parquet(percorso_file)

            # Estrai anno e mese dal nome del file
            nome_file = os.path.splitext(file)[0]
            parti_nome = nome_file.split('_')
            anno = int(parti_nome[1])
            mese = int(parti_nome[3])

            # Conta il numero di occorrenze di ciascuna tipologia
            conteggio_occorrenze = conta_occorrenze_professionisti(df).reset_index()
            conteggio_occorrenze.columns = ['tipologia_professionista_sanitario', 'conteggio']
            conteggio_occorrenze['anno'] = anno
            conteggio_occorrenze['mese'] = mese

            # Aggiungi al DataFrame aggregato
            dati_aggregati.append(conteggio_occorrenze)

    # Concatena tutti i risultati in un unico DataFrame
    df_aggregato = pd.concat(dati_aggregati, ignore_index=True)
    pd.set_option('display.max_rows', None)

    return df_aggregato

def crea_grafico_mensile_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    """
    Crea e salva il grafico a barre mensile per una specifica tipologia di professionista sanitario.
    :param df_professionista: dataFrame filtrato per la tipologia di professionista sanitario specificata
    :param tipologia_professionista (str): tipologia di professionista sanitario
    :param tipologia_dir (str): directory dove salvare i grafici
    :return None
    """
    # Percorso del file del grafico
    output_path = os.path.join(tipologia_dir, f'grafico_mensile_{tipologia_professionista}.png')

    # Verifica se il file esiste già
    if not os.path.isfile(output_path):
        plt.figure(figsize=(14, 8))
        sns.barplot(x='mese', y='conteggio', hue='anno', data=df_professionista, palette="coolwarm", dodge=True)

        # Configurazione delle etichette
        plt.title(f'Richieste Mensili per {tipologia_professionista}', fontsize=20, weight='bold')
        plt.xlabel('Mese', fontsize=14)
        plt.ylabel('Numero di Richieste', fontsize=14)
        plt.xticks(range(12), ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic'],
                   fontsize=12)
        plt.yticks(fontsize=12)
        plt.grid(True, linestyle='--', linewidth=0.5)
        plt.tight_layout()

        # Salva il grafico
        plt.savefig(output_path)
        plt.close()
        print(f"Grafico mensile salvato in '{output_path}'.")


def crea_istogrammi_mensili_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    """
    Crea e salva gli istogrammi annuali per una specifica tipologia di professionista sanitario.
    :param df_professionista: dataFrame filtrato per la tipologia specificata
    :param tipologia_professionista (str): tipologia di professionista sanitario
    :param tipologia_dir (str): directory dove salvare i grafici
    """
    # Estrai gli anni disponibili nel DataFrame
    anni = df_professionista['anno'].unique()

    # Creazione di un plot per ciascun anno
    for anno in anni:
        df_anno = df_professionista[df_professionista['anno'] == anno]

        # Percorso del file dell'istogramma
        output_path = os.path.join(tipologia_dir, f'istogramma_{tipologia_professionista}_anno{anno}.png')

        # Verifica se il file esiste già PRIMA di fare qualsiasi operazione
        if os.path.isfile(output_path):
            continue

        # Creazione dell'istogramma
        plt.figure(figsize=(14, 8))
        sns.histplot(data=df_anno, x='mese', weights='conteggio', bins=12, kde=False, color='skyblue', discrete=True)

        # Configurazione delle etichette
        plt.title(f'Frequenza delle Richieste Mensili per {tipologia_professionista} - Anno {anno}', fontsize=20,
                  weight='bold')
        plt.xlabel('Mese', fontsize=14)
        plt.ylabel('Frequenza', fontsize=14)
        plt.xticks(range(1, 13), ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic'],
                   fontsize=12)
        plt.yticks(fontsize=12)
        plt.grid(True, linestyle='--', linewidth=0.5)
        plt.tight_layout()

        # Salva l'istogramma
        plt.savefig(output_path)
        plt.close()

def crea_grafici_e_salva(df_aggregato, output_dir='grafici_professionisti'):
    """
    Crea e salva grafici a barre e istogrammi per ogni tipologia di professionista sanitario.
    :param df_aggregato: dataFrame aggregato contenente i dati mensili per ogni tipologia e anno.
    :param output_dir (str): directory principale dove salvare i grafici.
    """
    # Creare la cartella principale se non esiste
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Ottenere tutte le tipologie di professionisti sanitari
    tipologie = df_aggregato['tipologia_professionista_sanitario'].unique()

    for tipologia in tipologie:
        # Sostituire i caratteri non validi nei nomi delle cartelle
        tipologia_professionista = tipologia.replace('/', '_')

        # Creare una sottocartella per ogni tipologia
        tipologia_dir = os.path.join(output_dir, tipologia_professionista)
        if not os.path.exists(tipologia_dir):
            os.makedirs(tipologia_dir)

        # Filtrare il DataFrame per la tipologia specificata
        df_professionista = df_aggregato[df_aggregato['tipologia_professionista_sanitario'] == tipologia]

        # Assicurarsi che i mesi siano ordinati da 1 a 12
        df_professionista = df_professionista.sort_values(by=['mese', 'anno'])

        # Creare e salvare il grafico a barre
        crea_grafico_mensile_per_anni(df_professionista, tipologia_professionista, tipologia_dir)

        # Creare e salvare gli istogrammi per ogni anno
        crea_istogrammi_mensili_per_anni(df_professionista, tipologia_professionista, tipologia_dir)


def feature_extraction(df):
    """
    Aggiunge nuove features al DataFrame, elimina quelle ridondanti e crea dei grafici
    della richiesta di ogni professionista sanitario per ogni mese.
    :param df: dataFrame
    :return df: dataFrame
    """

    # Calcola l'età del paziente e rimuove la colonna 'data_nascita'
    df = extract_eta_paziente(df)
    df = remove_data_nascita(df)

    # Calcola la durata della televisita e rimuove le colonne dell'ora di inizio e fine erogazione
    df = extract_durata_televisita(df)
    df = remove_ora_erogazione(df)

    # Divide il dataset per anno e mese, e crea grafici della richiesta di professionisti per ogni mese
    df = extract_year_and_month(df)
    save_grouped_by_year_and_month(df)
    df_aggregato = conta_professionisti_per_mese('month_dataset')
    crea_grafici_e_salva(df_aggregato)

    # Salva il DataFrame aggregato in un file Parquet per ulteriori analisi
    df_aggregato.to_parquet('datasets/df_aggregato.parquet', index=False)
    return df
