import pandas as pd


def imputate_missing_values(df):
    """
    Imputa i valori mancanti del dataset df.
    :param df:
    :return:
    """
    # Visualizzo le statistiche dei valori mancanti prima dell'imputazione
    print("Statistiche valori mancanti prima dell'imputazione:\n")
    colonne_con_mancanti = df.columns[df.isnull().any()]
    print(df[colonne_con_mancanti].isnull().sum())
    print('-----------------------------------')

    # Imputazione dei valori mancanti relativi a comune_residenza
    df = imputate_comune_residenza(df)

    # Imputazione dei valori mancanti relativi a ora_inizio_erogazione e ora_fine_erogazione
    df = imputate_ora_inizio_erogazione_and_ora_fine_erogazione(df)

    # Rimozione dei campioni con 'data_disdetta' non nullo
    df = remove_disdette(df)

    # Visualizzo le statistiche dei valori mancanti dopo l'imputazione
    print("Statistiche valori mancanti dopo l'imputazione:\n")
    colonne_con_mancanti = df.columns[df.isnull().any()]
    print(df[colonne_con_mancanti].isnull().sum())
    print('-----------------------------------')

    return df


def remove_duplicati(df) -> pd.DataFrame:
    '''
    Rimuove i duplicati dal dataset df.
    :param df:
    :return:
    '''
    df.drop_duplicates(inplace=True)
    return df


def imputate_comune_residenza(df) -> pd.DataFrame:
    '''
    Imputa i valori mancanti per 'comune_residenza' del dataset df.
    :param df:
    :return:
    '''
    # Carico il dataset relativo ai codici ISTAT dei comuni italiani in modo da poter fare imputation
    df_istat = pd.read_excel('datasets/Codici-statistici-e-denominazioni-al-30_06_2024.xlsx')

    codice_comune_to_nome = pd.Series(df_istat['Denominazione in italiano'].values,
                                      index=df_istat['Codice Comune formato alfanumerico'])

    def fill_missing_comune_residenza(row):
        if row['comune_residenza'] is None:
            return codice_comune_to_nome.get(row['codice_comune_residenza'])
        return row['comune_residenza']

    df['comune_residenza'] = df.apply(fill_missing_comune_residenza, axis=1)

    '''
    # N.B. Dopo l'imputazione i valori mancanti relativi a comune_residenza continuano a risultare mancanti 
    in quanto relativi al comune di None in provincia di Torino con codice ISTAT 1168.
    '''

    return df


def remove_disdette(df) -> pd.DataFrame:
    '''
    Rimuove i campioni con 'data_disdetta' non nullo.
    :param df:
    :return: df senza campioni con 'data_disdetta' non nullo.
    '''
    df = df[df['data_disdetta'].isnull()]
    return df


def imputate_ora_inizio_erogazione_and_ora_fine_erogazione(df) -> pd.DataFrame:
    """
    Imputa i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' del dataset df.
    :param df:
    :return:
    """
    # Verifico se i valori mancanti sono relativi alle medesime righe del dataset:
    check_missing_values_same_row(df)

    # Conversione delle colonne 'ora_inizio_erogazione' e 'ora_fine_erogazione' in formato datetime
    df['ora_inizio_erogazione'] = pd.to_datetime(df['ora_inizio_erogazione'], errors='coerce', utc=True)
    df['ora_fine_erogazione'] = pd.to_datetime(df['ora_fine_erogazione'], errors='coerce', utc=True)

    # Calcolo della durata media delle attività per ciascun 'codice_descrizione_attivita'
    df_non_missing = df.dropna(subset=['ora_inizio_erogazione', 'ora_fine_erogazione']).copy()
    df_non_missing['durata'] = (
            df_non_missing['ora_fine_erogazione'] - df_non_missing['ora_inizio_erogazione']).dt.total_seconds()
    media_durata_sec = df_non_missing.groupby('codice_descrizione_attivita')['durata'].mean()
    media_durata = pd.to_timedelta(media_durata_sec, unit='s')

    # Converti la Series risultante in un dizionario
    media_durata_dict = media_durata.to_dict()

    # Itera attraverso ogni riga del DataFrame originale e imputa i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione'
    for index, row in df.iterrows():
        if pd.isnull(row['ora_inizio_erogazione']) and pd.isnull(row['ora_fine_erogazione']) and pd.isnull(
                row['data_disdetta']):
            codice_attivita = row['codice_descrizione_attivita']
            if codice_attivita in media_durata_dict:
                durata_media = media_durata_dict[codice_attivita]
                data_erogazione = pd.to_datetime(row['data_erogazione'], utc=True)
                df.at[index, 'ora_inizio_erogazione'] = data_erogazione.strftime('%Y-%m-%d %H:%M:%S%z')
                df.at[index, 'ora_fine_erogazione'] = (data_erogazione + durata_media).strftime('%Y-%m-%d %H:%M:%S%z')

    check_missing_values_start(df)
    check_missing_values_end(df)

    return df


def check_missing_values_same_row(df):
    '''
    Verifica se i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' riguardano le stesse righe.
    :param df:
    :return:
    '''
    missing_both = df['ora_inizio_erogazione'].isna() & df['ora_fine_erogazione'].isna()
    rows_with_both_missing = df[missing_both]
    num_rows_with_both_missing = len(rows_with_both_missing)
    print(f"Numero di righe con 'ora_inizio_erogazione' e 'ora_fine_erogazione' mancanti: {num_rows_with_both_missing}")


def check_missing_values_start(df):
    '''
    Verifica se ci sono valori mancanti per 'ora_inizio_erogazione'.
    :param df:
    :return:
    '''
    missing_start = df['ora_inizio_erogazione'].isna()
    rows_with_start_missing = df[missing_start]
    num_rows_with_start_missing = len(rows_with_start_missing)
    print(f"Numero di righe con 'ora_inizio_erogazione' mancante: {num_rows_with_start_missing}")


def check_missing_values_end(df):
    '''
    Verifica se ci sono valori mancanti per 'ora_fine_erogazione'.
    :param df:
    :return:
    '''
    missing_end = df['ora_fine_erogazione'].isna()
    rows_with_end_missing = df[missing_end]
    num_rows_with_end_missing = len(rows_with_end_missing)
    print(f"Numero di righe con 'ora_fine_erogazione' mancante: {num_rows_with_end_missing}")
