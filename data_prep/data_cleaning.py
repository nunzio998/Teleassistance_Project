import pandas as pd


def data_cleaning(df) -> pd.DataFrame:
    """
    Esegue le operazioni di pulizia del dataset df.
    1) imputazione dei valori mancanti e rimozione dei campioni con 'data_disdetta' non nullo.
    2) Identificazione e rimozione outliers.
    3) Gestione dei dati rumorosi.
    4) Rimozione dei duplicati.
    :param df:
    :return:
    """
    # Imputazione dei valori mancanti
    df = imputate_missing_values(df)

    # Rimozione dei campioni con 'data_disdetta' non nullo
    df = remove_disdette(df)

    # Identificazione e rimozione outliers dalle colonne specificate
    #df = identify_and_remove_outliers(df, ['ora_inizio_erogazione', 'ora_fine_erogazione'])

    # Gestione dei dati rumorosi nella colonna specificata
    #df = smooth_noisy_data(df, 'ora_inizio_erogazione')

    # Rimozione dei duplicati
    #df = remove_duplicati(df)

    #Ordina le date di erogazione del servizio
    df = ordina_date(df)

    # TODO: stabilire a quali features applicare 'identify_and_remove_outliers' e 'smooth_noisy_data'.

    return df


def imputate_missing_values(df) -> pd.DataFrame:
    """
    Imputa i valori mancanti del dataset df. Dopo una prima analisi si hanno i seguenti risultati:
    Statistiche valori mancanti prima dell'imputazione:

    codice_provincia_residenza      28380
    comune_residenza                  135
    codice_provincia_erogazione     28776
    ora_inizio_erogazione           28181
    ora_fine_erogazione             28181
    data_disdetta                  460639

    'codice_provincia_residenza' e 'codice_provincia_erogazione' non vengono imputati in quanto saranno rimossi
    successivamente. Inoltre anche 'data_disdetta' non viene imputato. L'imputazione fatta per 'comune_residenza' non
    produce risultati in quanto i valori mancanti sono relativi al comune di 'None', motivo per il quale non si può
    parlare di missing values. 'ora_inizio_erogazione' e 'ora_fine_erogazione' vengono imputati correttamente.
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

    # Visualizzo le statistiche dei valori mancanti dopo l'imputazione
    print("Statistiche valori mancanti dopo l'imputazione:\n")
    colonne_con_mancanti = df.columns[df.isnull().any()]
    print(df[colonne_con_mancanti].isnull().sum())
    print('-----------------------------------')

    return df


def remove_duplicati(df) -> pd.DataFrame:
    """
    Rimuove i duplicati dal dataset df.
    :param df:
    :return:
    """
    df.drop_duplicates(inplace=True)
    return df


def smooth_noisy_data(df, column, window_size=3):
    """
    Smooth noisy data using moving average.
    :param df: Il DataFrame originale.
    :param column: La colonna su cui applicare il smoothing.
    :param window_size: La dimensione della finestra per la media mobile.
    :return: Un DataFrame con i dati smussati.
    """
    if pd.api.types.is_datetime64_any_dtype(df[column]):
        # Convert datetime to timestamp
        df[column] = df[column].apply(lambda x: x.timestamp() if pd.notnull(x) else x)
        # Apply rolling mean
        df[column] = df[column].rolling(window=window_size, min_periods=1).mean()
        # Convert timestamp back to datetime
        df[column] = pd.to_datetime(df[column], unit='s', utc=True)
    else:
        df[column] = df[column].rolling(window=window_size, min_periods=1).mean()

    return df


def identify_and_remove_outliers(df, columns):
    """
    Identifica e rimuove outliers utilizzando il metodo IQR.
    :param df: Il DataFrame originale.
    :param columns: Le colonne su cui applicare la rimozione degli outliers.
    :return: Un DataFrame senza outliers.
    """
    for column in columns:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df


def imputate_comune_residenza(df) -> pd.DataFrame:
    """
    Imputa i valori mancanti per 'comune_residenza' del dataset df.
    :param df:
    :return:
    """
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
    """
    Rimuove i campioni con 'data_disdetta' non nullo.
    :param df:
    :return: df senza campioni con 'data_disdetta' non nullo.
    """
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

    # Conversione delle colonne 'data_erogazione', 'ora_inizio_erogazione' e 'ora_fine_erogazione' in formato datetime
    df['data_erogazione'] = pd.to_datetime(df['data_erogazione'], errors='coerce', utc=True)
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
    #print(media_durata_dict)

    # Funzione di supporto per l'imputazione
    def imputazione_riga(row):
        if pd.isnull(row['ora_inizio_erogazione']) and pd.isnull(row['ora_fine_erogazione']) and pd.isnull(
                row['data_disdetta']):
            codice_attivita = row['codice_descrizione_attivita']
            if codice_attivita in media_durata_dict:
                #print(f"data erogazione: {row['data_erogazione']}. Dati imputati: {row['ora_inizio_erogazione']} , {row['ora_fine_erogazione']}")
                durata_media = media_durata_dict[codice_attivita]
                data_erogazione = row['data_erogazione']
                row['ora_inizio_erogazione'] = data_erogazione
                row['ora_fine_erogazione'] = data_erogazione + durata_media
                #print(f"data erogazione: {row['data_erogazione']}. Dati imputati: {row['ora_inizio_erogazione']} , {row['ora_fine_erogazione']}")
        return row

    # Applica la funzione di imputazione a tutto il DataFrame
    df = df.apply(imputazione_riga, axis=1)
    #print(df.iloc[8])
    check_missing_values_start(df)
    check_missing_values_end(df)

    return df


def check_missing_values_same_row(df):
    """
    Verifica se i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' riguardano le stesse righe.
    :param df:
    :return:
    """
    missing_both = df['ora_inizio_erogazione'].isna() & df['ora_fine_erogazione'].isna()
    rows_with_both_missing = df[missing_both]
    num_rows_with_both_missing = len(rows_with_both_missing)
    print(f"Numero di righe con 'ora_inizio_erogazione' e 'ora_fine_erogazione' mancanti: {num_rows_with_both_missing}")


def check_missing_values_start(df):
    """
    Verifica se ci sono valori mancanti per 'ora_inizio_erogazione'.
    :param df:
    :return:
    """
    missing_start = df['ora_inizio_erogazione'].isna()
    rows_with_start_missing = df[missing_start]
    num_rows_with_start_missing = len(rows_with_start_missing)
    print(f"Numero di righe con 'ora_inizio_erogazione' mancante: {num_rows_with_start_missing}")


def check_missing_values_end(df):
    """
    Verifica se ci sono valori mancanti per 'ora_fine_erogazione'.
    :param df:
    :return:
    """
    missing_end = df['ora_fine_erogazione'].isna()
    rows_with_end_missing = df[missing_end]
    num_rows_with_end_missing = len(rows_with_end_missing)
    print(f"Numero di righe con 'ora_fine_erogazione' mancante: {num_rows_with_end_missing}")

def ordina_date(df):
    """

    :param df:
    :return:
    """
    # Assicurati che la colonna delle date sia in formato datetime
    df['data_erogazione'] = pd.to_datetime(df['data_erogazione'])
    # Ordina il DataFrame in base alla colonna delle date
    df= df.sort_values(by='data_erogazione')
    return df
