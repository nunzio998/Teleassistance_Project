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


import os
import pandas as pd


def calcola_quadrimestre(mese):
    """
    Assegna un quadrimestre ai mesi dell'anno.
    1-4 -> Quadrimestre 1
    5-8 -> Quadrimestre 2
    9-12 -> Quadrimestre 3
    """
    if 1 <= mese <= 4:
        return 1
    elif 5 <= mese <= 8:
        return 2
    elif 9 <= mese <= 12:
        return 3
    else:
        raise ValueError("Il valore del mese deve essere compreso tra 1 e 12")


def raggruppa_per_quadrimestre(cartella):
    """
    Raggruppa i file Parquet per quadrimestre e salva un file Parquet per ogni quadrimestre.

    Args: cartella (str): Il percorso della cartella contenente i file Parquet.

    Returns: None
    """
    # Dizionario per raccogliere i dati per ogni quadrimestre
    dati_per_quadrimestre = {}

    for file in os.listdir(cartella):
        if file.endswith(".parquet"):
            percorso_file = os.path.join(cartella, file)
            df = pd.read_parquet(percorso_file)

            # Estrai anno e mese dal nome del file
            nome_file = os.path.splitext(file)[0]
            parti_nome = nome_file.split('_')
            anno = int(parti_nome[1])
            mese = int(parti_nome[3])

            # Calcola il quadrimestre
            quadrimestre = calcola_quadrimestre(mese)

            # Crea una chiave per l'anno e il quadrimestre
            chiave = (anno, quadrimestre)

            # Aggiungi i dati al dizionario
            if chiave not in dati_per_quadrimestre:
                dati_per_quadrimestre[chiave] = []

            dati_per_quadrimestre[chiave].append(df)

    # Combina i dati per ciascun quadrimestre e salva un file Parquet
    for (anno, quadrimestre), liste_dati in dati_per_quadrimestre.items():
        df_quadrimestrale = pd.concat(liste_dati, ignore_index=True)
        output_path = os.path.join(cartella, f'Anno_{anno}_Quadrimestre_{quadrimestre}.parquet')
        df_quadrimestrale.to_parquet(output_path, index=False)
        print(f"File salvato: {output_path}")

def conta_occorrenze_professionisti(df, colonne=['tipologia_professionista_sanitario']):
    """
    Conta il numero di occorrenze per le combinazioni di valori nelle colonne specificate.

    Args:
    df (pd.DataFrame): Il DataFrame su cui effettuare il conteggio.
    colonne (list): Una lista delle colonne su cui raggruppare i dati per il conteggio.

    Returns:
    pd.DataFrame: Un DataFrame con il conteggio delle occorrenze per ogni combinazione delle colonne specificate.
    """
    return df.groupby(colonne).size().reset_index(name='conteggio')


def conta_professionisti_per_mese(cartella):
    """
    Conta per ogni mese (file Parquet) il numero di volte in cui compare ogni professionista sanitario.

    Args:
    cartella (str): Il percorso della cartella contenente i file Parquet.

    Returns:
    pd.DataFrame: Un DataFrame contenente il numero di occorrenze per ogni tipologia di professionista
                  per ogni mese.
    """
    dati_aggregati = []
    for file in os.listdir(cartella):
        if file.endswith(".parquet"):
            percorso_file = os.path.join(cartella, file)
            df = pd.read_parquet(percorso_file)
            nome_file = os.path.splitext(file)[0]
            parti_nome = nome_file.split('_')
            anno = int(parti_nome[1])
            mese = int(parti_nome[3])
            conteggio_occorrenze = conta_occorrenze_professionisti(df, colonne=['tipologia_professionista_sanitario'])
            conteggio_occorrenze['anno'] = anno
            conteggio_occorrenze['mese'] = mese
            dati_aggregati.append(conteggio_occorrenze)

    df_aggregato = pd.concat(dati_aggregati, ignore_index=True)
    df_aggregato.to_csv('dati_aggregati_professionisti_per_mese.csv', index=False)
    return df_aggregato

def conta_professionisti_per_sesso(cartella):
    dati_aggregati = []
    for file in os.listdir(cartella):
        if file.endswith(".parquet"):
            percorso_file = os.path.join(cartella, file)
            df = pd.read_parquet(percorso_file)
            nome_file = os.path.splitext(file)[0]
            parti_nome = nome_file.split('_')
            anno = int(parti_nome[1])
            conteggio_occorrenze = conta_occorrenze_professionisti(df, colonne=['sesso', 'tipologia_professionista_sanitario'])
            conteggio_occorrenze['anno'] = anno
            dati_aggregati.append(conteggio_occorrenze)

    df_aggregato = pd.concat(dati_aggregati, ignore_index=True)
    df_aggregato.to_csv('dati_aggregati_professionisti_per_sesso.csv', index=False)
    return df_aggregato



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
    raggruppa_per_quadrimestre('month_dataset')

    return df
