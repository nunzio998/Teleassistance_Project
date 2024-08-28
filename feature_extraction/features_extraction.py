from datetime import datetime
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

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

def crea_grafico_mensile_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    """
       Crea e salva il grafico a barre mensile per una specifica tipologia di professionista sanitario.

       Args:
       df_professionista (pd.DataFrame): Il DataFrame filtrato per la tipologia specificata.
       tipologia_professionista (str): La tipologia di professionista sanitario con caratteri non validi sostituiti.
       tipologia_dir (str): La directory dove salvare i grafici.
       """
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
    plt.savefig(os.path.join(tipologia_dir, f'grafico_mensile_{tipologia_professionista}.png'))
    plt.close()

def crea_grafico_sesso_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    plt.figure(figsize=(14, 8))
    sns.barplot(x='sesso', y='conteggio', hue='anno', data=df_professionista, palette="coolwarm", dodge=True)
    plt.title(f'Richieste per {tipologia_professionista} in base al Sesso', fontsize=20, weight='bold')
    plt.xlabel('Sesso', fontsize=14)
    plt.ylabel('Numero di Richieste', fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid(True, linestyle='--', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(tipologia_dir, f'grafico_sesso_{tipologia_professionista}.png'))
    plt.close()

def crea_istogrammi_mensili_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    """
    Crea e salva gli istogrammi annuali per una specifica tipologia di professionista sanitario.

    Args:
    df_professionista (pd.DataFrame): Il DataFrame filtrato per la tipologia specificata.
    tipologia_professionista (str): La tipologia di professionista sanitario con caratteri non validi sostituiti.
    tipologia_dir (str): La directory dove salvare i grafici.
    """
    # Estrai gli anni disponibili nel DataFrame
    anni = df_professionista['anno'].unique()

    # Creazione di un plot per ciascun anno
    for anno in anni:
        df_anno = df_professionista[df_professionista['anno'] == anno]

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
        plt.savefig(os.path.join(tipologia_dir, f'istogramma_{tipologia_professionista}_anno_{anno}.png'))
        plt.close()

def crea_grafici_e_salva(df_aggregato, output_dir='grafici_professionisti',tipo='mese'):
    """
    Crea e salva grafici a barre e istogrammi per ogni tipologia di professionista sanitario.

    Args:
    df_aggregato (pd.DataFrame): Il DataFrame aggregato contenente i dati mensili per ogni tipologia e anno.
    output_dir (str): La directory principale dove salvare i grafici.
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

        df_professionista = df_aggregato[df_aggregato['tipologia_professionista_sanitario'] == tipologia]

        if tipo == 'mese':
            df_professionista = df_professionista.sort_values(by=['mese', 'anno'])
            crea_grafico_mensile_per_anni(df_professionista, tipologia_professionista, tipologia_dir)
            crea_istogrammi_mensili_per_anni(df_professionista, tipologia_professionista, tipologia_dir)
        elif tipo == 'sesso':
            crea_grafico_sesso_per_anni(df_professionista, tipologia_professionista, tipologia_dir)

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

    df_aggregato = conta_professionisti_per_mese('month_dataset')
    crea_grafici_e_salva(df_aggregato,output_dir='grafici_professionisti_mese', tipo='mese')

    df_aggregato_sesso = conta_professionisti_per_sesso('month_dataset')
    crea_grafici_e_salva(df_aggregato_sesso, output_dir='grafici_professionisti_per_sesso', tipo='sesso')

    return df
