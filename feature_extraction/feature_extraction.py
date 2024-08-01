import pandas as pd
from datetime import datetime


def extract_durata_televisita(df):
    """
    Calcola la durata della televisita tramite l'ora di inizio erogazione e l'ora di fine erogazione
    :param df:
    :return: dataframe
    """
    # Calcolare la durata della televisita
    # Assicurarsi che 'ora_inizio_erogazione' e 'ora_fine_erogazione' siano in formato datetime
    df['ora_inizio_erogazione'] = pd.to_datetime(df['ora_inizio_erogazione'], errors='coerce')
    df['ora_fine_erogazione'] = pd.to_datetime(df['ora_fine_erogazione'], errors='coerce')

    # Calcolare la durata in minuti
    df['durata_televisita'] = (df['ora_fine_erogazione'] - df['ora_inizio_erogazione']).dt.total_seconds() / 60

    return df


def extract_eta_paziente(df):
    """
    Calcola l'età del paziente tramite la data di nascita
    :param df:
    :return:
    """
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
    return df
