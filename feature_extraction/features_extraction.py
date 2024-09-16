from datetime import datetime
import pandas as pd
import os
import logging

# Configuro il logger
logging.basicConfig(level=logging.INFO,  # Imposto il livello minimo di log
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Formato del log

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

    # Aggiunge al dataset il conteggio della richiesta di ogni professionista per ogni mese
    df_aggregato = conta_professionisti_per_mese('month_dataset')
    df_aggregato.to_parquet('datasets/df_aggregato.parquet', index=False)

    return df

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
            return int(durata.total_seconds() / 60)
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
        logging.info(f"Directory '{directory}' creata.")

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
