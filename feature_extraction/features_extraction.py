from datetime import datetime
import pandas as pd
import os

def extract_durata_televisita(df):
    # Calcolare la durata della televisita
    # Assicurarsi che 'ora_inizio_erogazione' e 'ora_fine_erogazione' siano in formato datetime
    df['ora_inizio_erogazione'] = pd.to_datetime(df['ora_inizio_erogazione'], errors='coerce')
    df['ora_fine_erogazione'] = pd.to_datetime(df['ora_fine_erogazione'], errors='coerce')

    # Funzione per calcolare la durata della televisita

    def calcola_durata(row):
        """
        Calcola la durata della televisita in secondi tra 'ora_inizio_erogazione' e 'ora_fine_erogazione'.
        :param row: Una riga del DataFrame.
        :return: Durata in secondi.
        """
        if pd.notnull(row['ora_inizio_erogazione']) and pd.notnull(row['ora_fine_erogazione']):
            durata = row['ora_fine_erogazione'] - row['ora_inizio_erogazione']
            return int(durata.total_seconds()/60)
        else:
            return None

    # Calcolare la durata in minuti
    #df['durata_televisita'] = (df['ora_fine_erogazione'] - df['ora_inizio_erogazione']).dt.total_seconds() / 60
    df['durata_televisita'] = df.apply(calcola_durata, axis=1)

    return df

def extract_eta_paziente(df):
    # Assicurarsi che 'data_nascita' sia in formato datetime
    df['data_nascita'] = pd.to_datetime(df['data_nascita'], errors='coerce')

    # Calcolare l'età in anni
    current_date = datetime.now()

    # Funzione per calcolare l'età
    def calcola_eta(row):
        birth_date = row['data_nascita']
        if pd.isnull(birth_date):
            return None
        age = current_date.year - birth_date.year - (
                (current_date.month, current_date.day) < (birth_date.month, birth_date.day))
        return age

    df['eta_paziente'] = df.apply(calcola_eta, axis=1)

    return df




def extract_timestamp(df):
    """
    Estrae il timestamp di quando è avvenuta la visita nel tempo
    :param df:
    :return:
    """

    return df


def extract_incremento(df):
    """
    Aggiunge una nuova feature al DataFrame: incremento.
    :param df:
    :return:
    """
    # TODO: Implementare la funzione
    return df

def extract_year_and_month(df):
    '''
    Questa funzione estrae l'anno e il mese dalla colonna 'data_erogazione' di un DataFrame,
    crea nuove colonne 'year' e 'month', e poi raggruppa il DataFrame per anno e mese.
    Ogni gruppo viene salvato in un file Parquet separato nella directory 'month_dataset'.
    Successivamente, la funzione carica automaticamente tutti i file Parquet nella directory.

    Parametri:
    df (DataFrame): Il DataFrame originale contenente la colonna 'data_erogazione'.

    Ritorna:
    DataFrame: Il DataFrame originale con le nuove colonne 'year' e 'month'.
    '''
    # Crea nuove colonne 'year' e 'month' estraendo l'anno e il mese dalla colonna 'data_erogazione'
    df['year'] = df['data_erogazione'].dt.year
    df['month'] = df['data_erogazione'].dt.month

    # Raggruppa il DataFrame per anno e mese e salva ogni gruppo in un file Parquet separato
    for (year, month), group in df.groupby(['year', 'month']):
        output_path = f'month_dataset/Anno_{year}_Mese_{month}.parquet'
        group.to_parquet(output_path, index=False)

    # Caricamento automatico dei file Parquet per ogni anno e mese
    directory = 'month_dataset'
    for file_name in os.listdir(directory):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(directory, file_name)
            df = pd.read_parquet(file_path)
    return df


def conta_professionisti_per_mese(cartella):
    """
    Conta per ogni mese (file Parquet) il numero di volte in cui compare ogni professionista sanitario.

    Args:
    cartella (str): Il percorso della cartella contenente i file Parquet.

    Returns:
    dict: Un dizionario dove le chiavi sono i nomi dei file (mesi) e i valori sono Serie pandas
          con le tipologie di professionisti e il numero di occorrenze.
    """
    # Dizionario per contenere i risultati
    risultati_per_mese = {}

    def extract_tipologia_televisite(df, colonna='tipologia_professionista_sanitario'):
        """
        Estrae tutte le tipologie uniche di professionisti sanitari in un DataFrame.
        """
        return df[colonna].unique().tolist()

    def conta_occorrenze_professionisti(df, colonna='tipologia_professionista_sanitario'):
        """
        Conta il numero di occorrenze di ciascuna tipologia di professionista sanitario in un DataFrame.
        """
        return df[colonna].value_counts()

    # Scorri tutti i file Parquet nella cartella
    for file in os.listdir(cartella):
        if file.endswith(".parquet"):
            percorso_file = os.path.join(cartella, file)

            # Leggi il file Parquet in un DataFrame
            df = pd.read_parquet(percorso_file)

            # Estrai le tipologie uniche di professionisti sanitari
            tipologie_uniche = extract_tipologia_televisite(df)

            # Conta il numero di occorrenze di ciascuna tipologia
            conteggio_occorrenze = conta_occorrenze_professionisti(df)

            # Salva il risultato per il mese (nome file senza estensione)
            nome_mese = os.path.splitext(file)[0]
            risultati_per_mese[nome_mese] = conteggio_occorrenze

    return risultati_per_mese



def feature_extraction(df):
    """
    Aggiunge nuove features al DataFrame: età del paziente e durata della televisita.
    :param df: Il DataFrame originale.
    :return: Il DataFrame con le nuove features.
    """
    # Calcolare l'età del paziente
    df = extract_eta_paziente(df)
    # Calcolare la durata della televisita
    df = extract_durata_televisita(df)
    # Divisione dataset per anno e mese, e salvataggio in file Parquet
    df = extract_year_and_month(df)
    risultati_per_mese= conta_professionisti_per_mese('month_dataset')
    print(risultati_per_mese)

    return df
