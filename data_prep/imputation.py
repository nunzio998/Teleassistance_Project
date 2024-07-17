import pandas as pd
from fancyimpute import IterativeImputer
from sklearn.preprocessing import LabelEncoder


def imputate_missing_values(df):
    '''
    Imputa i valori mancanti del dataset df.
    :param df:
    :return:
    '''

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


def imputate_ora_inizio_erogazione_and_ora_fine_erogazione(df) -> pd.DataFrame:
    '''
    Imputa i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' del dataset df.
    :param df:
    :return:
    '''
    # Verifico se i valori mancanti sono relativi alle medesime righe del dataset:
    check_missing_values_same_row(df)
    df.to_csv('datasets/df_missing_values_same_row.csv', index=False)
    # Ho verificato che le occorrenze di valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' riguardano le stesse righe.
    # Quindi, dove manca uno dei due valori manca anche l'altro. Pertanto, posso procedere con l'imputazione dei valori mancanti.

    # Ora, per ogni tipologia di teleassistenza, calcolo la media delle ore di inizio erogazione. Questo valore sarà utilizzato per l'imputazione.

    # Conversione della colonna 'ora_inizio_erogazione' in formato datetime
    df['ora_inizio_erogazione'] = pd.to_datetime(df['ora_inizio_erogazione'], errors='coerce', utc=True)

    # Filtrare il DataFrame per rimuovere le righe con valori mancanti in 'ora_inizio_erogazione'
    df_non_missing = df.dropna(subset=['ora_inizio_erogazione'])

    # Converti i timestamp in secondi dall'epoca Unix usando .loc
    df_non_missing.loc[:, 'ora_inizio_erogazione_sec'] = df_non_missing['ora_inizio_erogazione'].apply(
        lambda x: x.timestamp())

    # Raggruppa il DataFrame filtrato per 'codice_descrizione_attivita' e calcola la media dei secondi
    media_ora_inizio_sec = df_non_missing.groupby('codice_descrizione_attivita')['ora_inizio_erogazione_sec'].mean()

    # Converti la media dei secondi in datetime
    media_ora_inizio = pd.to_datetime(media_ora_inizio_sec, unit='s', utc=True)

    # Formatta il datetime nel formato richiesto '2019-02-04T11:00:28+01:00'
    media_ora_inizio_formatted = media_ora_inizio.dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    # Converti la Series risultante in un dizionario
    media_ora_inizio_dict = media_ora_inizio_formatted.to_dict()

    # Itera attraverso ogni riga del DataFrame originale e imputa i valori mancanti per 'ora_inizio_erogazione'
    for index, row in df.iterrows():
        if pd.isnull(row['ora_inizio_erogazione']):  # Controlla se è mancante
            codice_attivita = row['codice_descrizione_attivita']
            if codice_attivita in media_ora_inizio_dict:  # Controlla se c'è una corrispondenza nel dizionario
                df.at[index, 'ora_inizio_erogazione'] = pd.to_datetime(media_ora_inizio_dict[codice_attivita])

    check_missing_values_start(df)


    # TODO Imputazione dei valori mancanti per 'ora_fine_erogazione'


    check_missing_values_end(df)

    return df


def check_missing_values_same_row(df):
    '''
    Verifica se i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' riguardano le stesse righe.
    :param df:
    :return:
    '''
    # Creazione di una maschera booleana per identificare le righe con entrambi i valori mancanti
    missing_both = df['ora_inizio_erogazione'].isna() & df['ora_fine_erogazione'].isna()

    # Filtrare il DataFrame per ottenere solo le righe con entrambi i valori mancanti
    rows_with_both_missing = df[missing_both]

    # Contare il numero di righe con entrambi i valori mancanti
    num_rows_with_both_missing = len(rows_with_both_missing)

    print(f"Numero di righe con 'ora_inizio_erogazione' e 'ora_fine_erogazione' mancanti: {num_rows_with_both_missing}")
    # print(rows_with_both_missing)


def check_missing_values_start(df):
    '''
    Verifica se i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' riguardano le stesse righe.
    :param df:
    :return:
    '''
    # Creazione di una maschera booleana per identificare le righe con entrambi i valori mancanti
    missing_both = df['ora_inizio_erogazione'].isna()

    # Filtrare il DataFrame per ottenere solo le righe con entrambi i valori mancanti
    rows_with_both_missing = df[missing_both]

    # Contare il numero di righe con entrambi i valori mancanti
    num_rows_with_both_missing = len(rows_with_both_missing)

    print(f"Numero di righe con 'ora_inizio_erogazione' e 'ora_fine_erogazione' mancanti: {num_rows_with_both_missing}")
    # print(rows_with_both_missing)


def check_missing_values_end(df):
    '''
    Verifica se i valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione' riguardano le stesse righe.
    :param df:
    :return:
    '''
    # Creazione di una maschera booleana per identificare le righe con entrambi i valori mancanti
    missing_both = df['ora_fine_erogazione'].isna()

    # Filtrare il DataFrame per ottenere solo le righe con entrambi i valori mancanti
    rows_with_both_missing = df[missing_both]

    # Contare il numero di righe con entrambi i valori mancanti
    num_rows_with_both_missing = len(rows_with_both_missing)

    print(f"Numero di righe con 'ora_inizio_erogazione' e 'ora_fine_erogazione' mancanti: {num_rows_with_both_missing}")
    # print(rows_with_both_missing)
