import pandas as pd

def dati_da_utilizzare(df):
    """
    Definisce i dati da utilizzare per calcolare l'incremento

    Args:
            df: DataFrame contenente i dati iniziali
    Returns:
            tipologie: lista delle tipologie di professionista sanitario
            DaF: DataFrame che contiene il numero di occorrenze di ogni professionista sanitario per mese
            intervalli_anni_mesi: lista di tuple che definiscono intervalli di anni e mesi in formato semestrale
        """

    tipologie = df['tipologia_professionista_sanitario'].unique()  # estrae le tipologie uniche di professionista

    DaF = pd.read_parquet('datasets/df_aggregato.parquet') # lettura del file contente le occorrenze

    # Lista degli intervalli di anni e mesi in formato semestrale
    intervalli_anni_mesi = [
        (2019, 2020, '1, 2, 3, 4, 5, 6'),
        (2019, 2020, '7, 8, 9, 10, 11, 12'),
        (2020, 2021, '1, 2, 3, 4, 5, 6'),
        (2020, 2021, '7, 8, 9, 10, 11, 12'),
        (2021, 2022, '1, 2, 3, 4, 5, 6'),
        (2021, 2022, '7, 8, 9, 10, 11, 12')
    ]
    return tipologie, DaF, intervalli_anni_mesi

def get_intervallo_mesi(mese):
    """
    Restituisce l'intervallo di mesi a cui appartiene il mese specificato.

    Args:
           mese: Mese per il quale si desidera ottenere l'intervallo
    Returns:
           intervallo: Intervallo di mesi in cui il mese rientra
       """
    intervalli_mesi = {
            (1, 2, 3, 4, 5, 6): '1, 2, 3, 4, 5, 6',
            (7, 8, 9, 10, 11, 12): '7, 8, 9, 10, 11, 12'
        }
    for mesi, intervallo in intervalli_mesi.items():
        if mese in mesi:
            return intervallo
    return "Intervallo non trovato"

def somma_per_intervallo_mesi(df, tipologie):
    """
       Calcola la somma delle occorrenze di ogni professionista sanitario per l'intervallo di
       mesi specificato (semestre). I risultati vengono raggruppati in un dataframe che ha come colonne
       'tipologia_professionista_sanitario', 'anno', 'intervallo_mesi' e 'conteggio'.

       Args:
           df: DataFrame contenente i dati
           tipologie: Lista di tipologie di professionisti sanitari

       Returns:
           risultato: DataFrame con la somma delle occorrenze per tipologia, anno e intervallo di mesi
       """

    df['intervallo_mesi'] = df['mese'].apply(get_intervallo_mesi)  # Aggiungo la colonna 'intervallo_mesi'

    # Verifico che le tipolgie di professionista sanitario siano quelle estratte in precedenza
    df_filtrato = df[df['tipologia_professionista_sanitario'].isin(tipologie)]

    # Raggruppa e somma le occorrenze dei dati che hanno stesso tipologia di professionista sanitario, anno e intervallo di mesi
    risultato = df_filtrato.groupby(['tipologia_professionista_sanitario', 'anno', 'intervallo_mesi'])[
        'conteggio'].sum().reset_index()

    return risultato


def calcola_incremento(df,intervalli_anni_mesi):
    """
    Crea una tabella che ha come colonne 'tipologia_professionista_sanitario', 'intervallo_mesi', e 'anno',
    e calcola l'incremento percentuale fra due anni successivi.


        Args:
            df: DataFrame contenente i dati raggruppati per anno e mese
            intervalli_anni_mesi: Intervalli di anni e mesi per cui calcolare l'incremento
        Returns:
            risultato_con_incremento: DataFrame contenente l'incremento percentuale per ciascun intervallo

        """

    # Crea una tabella pivot per avere anni come colonne
    df_pivot = df.pivot_table(index=['tipologia_professionista_sanitario', 'intervallo_mesi'], columns='anno',
                              values='conteggio').reset_index()
    def incremento_percentuale(row, anno1, anno2):
        """
        Calcola l'incremento percentuale del conteggio di professionisti sanitari tra due anni consecutivi.

        """
        conteggio_anno1 = row.get(anno1, 0)
        conteggio_anno2 = row.get(anno2, 0)

        # Calcolo dell'incremento percentuale
        return ((conteggio_anno2 - conteggio_anno1) / conteggio_anno1) * 100 if conteggio_anno1 > 0 else float('inf')

    risultati = []
    # Itera su ciascuna coppia di 'anno' e 'intervallo mesi' presente in 'intervalli_anni_mesi'.
    for (anno1, anno2, intervallo) in intervalli_anni_mesi:
        df_intervallo = df_pivot[df_pivot['intervallo_mesi'] == intervallo]

        # Itera su ciascuna riga del DataFrame filtrato.
        for _, row in df_intervallo.iterrows():
            incremento = incremento_percentuale(row, anno1, anno2)  # Calcola l'incremento percentuale tra 'anno1' e 'anno2'
            classificazione = classifica_incremento(incremento) # Classifica l'incremento in una delle categorie: alto, medio, basso, costante

            # Aggiunge un dizionario con i risultati per la riga corrente nella lista 'risultati'.
            risultati.append({
                'tipologia_professionista_sanitario': row['tipologia_professionista_sanitario'],
                'anno': f"{anno1}-{anno2}",
                'intervallo_mesi': intervallo,
                'incremento_percentuale': incremento,
                'incremento':classificazione
            })
    risultato_con_incremento = pd.DataFrame(risultati)

    return risultato_con_incremento


def classifica_incremento(percentuale):
    """
    Classifica la percentuale dell'incremento in "alta", "media", "bassa" e "costante".

    Args:
        percentuale (float): La percentuale di incremento da classificare.

    Returns:
        str: La classificazione dell'incremento ('alta', 'media', 'costante', 'bassa').
    """

    if percentuale < 0:
        return 'costante'
    elif 0 <= percentuale <= 35:
        return 'bassa'
    elif 35 < percentuale <= 80:
        return 'media'
    else:
        return 'alta'


def estendi_incremento(df):
    """
    Associa l'incremento associato a ciascun intervallo di mesi, assegnandolo a ogni mese individualmente.
    Per fare questo, considera il dataFrame che contiene l'incremento percentuale per intervalli di mesi
    e lo "estende": divide gli intervalli di mesi in mesi separati e associa l'incremento ad ognuno di essi.

    Args:
        df: dataframe
    Returns:
        df_esteso: dataframe in cui ad ogni mese è associato un valore di incremento
    """
    # Crea un DataFrame vuoto per i dati estesi
    dati_estesi = []

    # Itera su ogni riga del DataFrame esistente
    for _, row in df.iterrows():
        # Considera due anni consecutivi e prende solo il secondo (es. 2019-2020, prende 2020)
        anno_intervallo = row['anno'].split('-')
        anno_fine = int(anno_intervallo[1])

        # Ottieni l'intervallo di mesi come lista (es. ['1', '2', '3', ...])
        mesi = row['intervallo_mesi'].split(', ')

        # Aggiungi una riga per ogni mese dell'intervallo
        for mese in mesi:
            dati_estesi.append({
                'tipologia_professionista_sanitario': row['tipologia_professionista_sanitario'],
                'year': anno_fine,
                'month': int(mese),
                'incremento_percentuale': row['incremento_percentuale'],
                'incremento':row['incremento']
            })

    # Converti la lista di dizionari in un DataFrame
    df_esteso = pd.DataFrame(dati_estesi)

    # Ordina i dati per tipologia, anno e mese
    df_esteso = df_esteso.sort_values(by=['tipologia_professionista_sanitario', 'year', 'month']).reset_index(drop=True)

    return df_esteso


def unisci_incremento(df_originale, risultato_esteso):
    # Unisci il DataFrame originale con il risultato esteso, mantenendo tipologia, anno e mese
    df_unito = pd.merge(df_originale,
                        risultato_esteso[['tipologia_professionista_sanitario', 'year', 'month', 'incremento']],
                        on=['tipologia_professionista_sanitario', 'year', 'month'],
                        how='left')

    df_unito.loc[df_unito['year'] == 2019, 'incremento'] = 'costante'

    return df_unito

def incremento(df):

    tipologie, DaF, intervalli_anni_mese = dati_da_utilizzare(df)
    print("Calcolo la variabile incremento...")
    risultato = somma_per_intervallo_mesi(DaF, tipologie)
    risultato_con_incremento = calcola_incremento(risultato, intervalli_anni_mese)
    risultato_esteso = estendi_incremento(risultato_con_incremento)

    # Salva il DataFrame esteso in un nuovo file
    risultato_esteso.to_parquet('datasets/df_incremento_percentuale_esteso.parquet', index=False)
    #print(risultato_esteso.head(500).to_string())

    # Unisci la colonna incremento al DataFrame originale
    df_finale = unisci_incremento(df, risultato_esteso)

    # Elimina tutte le righe dove l'anno è 2019
    df_finale = df_finale[df_finale['year'] != 2019]

    return df_finale


























"""
                                             tipologia  mese       anno  percentuale incremento
0                                           Infermiere   1,4  2019,2020   115.818092       alta
1                                           Infermiere   5,8  2019,2020    38.422351   costante
2                                           Infermiere  9,12  2019,2020    79.069648      media
3                                           Infermiere   1,4  2020,2021    51.094824      media
4                                           Infermiere   5,8  2020,2021     7.337388   costante
5                                           Infermiere  9,12  2020,2021   -30.465416      bassa
6                                           Infermiere   1,4  2021,2022   -31.421295      bassa
7                                           Infermiere   5,8  2021,2022   -16.122280      bassa
8                                           Infermiere  9,12  2021,2022    -8.365598      bassa
9                                            Psicologo   1,4  2019,2020   140.305149       alta
10                                           Psicologo   5,8  2019,2020    43.310386   costante
11                                           Psicologo  9,12  2019,2020    71.346627      media
12                                           Psicologo   1,4  2020,2021    48.862434   costante
13                                           Psicologo   5,8  2020,2021    11.027569   costante
14                                           Psicologo  9,12  2020,2021   -19.033955      bassa
15                                           Psicologo   1,4  2021,2022   -20.437178      bassa
16                                           Psicologo   5,8  2021,2022   -12.509406      bassa
17                                           Psicologo  9,12  2021,2022   -13.723174      bassa
18                                            Dietista   1,4  2019,2020   139.704939       alta
19                                            Dietista   5,8  2019,2020    53.622207      media
20                                            Dietista  9,12  2019,2020   101.484169       alta
21                                            Dietista   1,4  2020,2021    59.379181      media
22                                            Dietista   5,8  2020,2021     9.189070   costante
23                                            Dietista  9,12  2020,2021   -30.463251      bassa
24                                            Dietista   1,4  2021,2022   -28.912021      bassa
25                                            Dietista   5,8  2021,2022   -11.019173      bassa
26                                            Dietista  9,12  2021,2022    -5.202448      bassa
27                                      Fisioterapista   1,4  2019,2020   149.233716       alta
28                                      Fisioterapista   5,8  2019,2020    46.303901   costante
29                                      Fisioterapista  9,12  2019,2020    87.737478      media
30                                      Fisioterapista   1,4  2020,2021    61.337433      media
31                                      Fisioterapista   5,8  2020,2021    24.491228   costante
32                                      Fisioterapista  9,12  2020,2021   -19.641214      bassa
33                                      Fisioterapista   1,4  2021,2022   -30.014293      bassa
34                                      Fisioterapista   5,8  2021,2022   -24.464487      bassa
35                                      Fisioterapista  9,12  2021,2022   -13.165426      bassa
36                                Assistente sanitario   1,4  2019,2020   146.640316       alta
37                                Assistente sanitario   5,8  2019,2020    28.490028   costante
38                                Assistente sanitario  9,12  2019,2020    78.473581      media
39                                Assistente sanitario   1,4  2020,2021    27.483974   costante
40                                Assistente sanitario   5,8  2020,2021     3.325942   costante
41                                Assistente sanitario  9,12  2020,2021   -26.589912      bassa
42                                Assistente sanitario   1,4  2021,2022   -28.975487      bassa
43                                Assistente sanitario   5,8  2021,2022   -29.113019      bassa
44                                Assistente sanitario  9,12  2021,2022   -22.479462      bassa
45                                         Logopedista   1,4  2019,2020    85.096154      media
46                                         Logopedista   5,8  2019,2020    42.626070   costante
47                                         Logopedista  9,12  2019,2020    98.802395      media
48                                         Logopedista   1,4  2020,2021    74.545455      media
49                                         Logopedista   5,8  2020,2021    28.552368   costante
50                                         Logopedista  9,12  2020,2021   -16.967871      bassa
51                                         Logopedista   1,4  2021,2022   -26.537698      bassa
52                                         Logopedista   5,8  2021,2022   -16.502335      bassa
53                                         Logopedista  9,12  2021,2022    -3.990326      bassa
54                                         Ostetrica/o   1,4  2019,2020   182.264151       alta
55                                         Ostetrica/o   5,8  2019,2020    57.824427      media
56                                         Ostetrica/o  9,12  2019,2020    67.027864      media
57                                         Ostetrica/o   1,4  2020,2021    41.042781   costante
58                                         Ostetrica/o   5,8  2020,2021     3.627570   costante
59                                         Ostetrica/o  9,12  2020,2021   -39.017609      bassa
60                                         Ostetrica/o   1,4  2021,2022   -37.440758      bassa
61                                         Ostetrica/o   5,8  2021,2022   -17.736289      bassa
62                                         Ostetrica/o  9,12  2021,2022    -8.966565      bassa
63                             Terapista Occupazionale   1,4  2019,2020   188.349515       alta
64                             Terapista Occupazionale   5,8  2019,2020    41.743119   costante
65                             Terapista Occupazionale  9,12  2019,2020    55.681818      media
66                             Terapista Occupazionale   1,4  2020,2021    31.649832   costante
67                             Terapista Occupazionale   5,8  2020,2021     9.385113   costante
68                             Terapista Occupazionale  9,12  2020,2021   -30.900243      bassa
69                             Terapista Occupazionale   1,4  2021,2022   -35.549872      bassa
70                             Terapista Occupazionale   5,8  2021,2022   -23.668639      bassa
71                             Terapista Occupazionale  9,12  2021,2022    -1.056338      bassa
72                             Educatore Professionale   1,4  2019,2020    80.882353      media
73                             Educatore Professionale   5,8  2019,2020    36.150235   costante
74                             Educatore Professionale  9,12  2019,2020    43.253968   costante
75                             Educatore Professionale   1,4  2020,2021    28.861789   costante
76                             Educatore Professionale   5,8  2020,2021     8.620690   costante
77                             Educatore Professionale  9,12  2020,2021   -31.855956      bassa
78                             Educatore Professionale   1,4  2021,2022   -21.766562      bassa
79                             Educatore Professionale   5,8  2021,2022   -28.253968      bassa
80                             Educatore Professionale  9,12  2021,2022   -15.040650      bassa
81                                            Podologo   1,4  2019,2020   109.219858       alta
82                                            Podologo   5,8  2019,2020    12.156863   costante
83                                            Podologo  9,12  2019,2020    61.316872      media
84                                            Podologo   1,4  2020,2021    26.779661   costante
85                                            Podologo   5,8  2020,2021     7.342657   costante
86                                            Podologo  9,12  2020,2021   -26.020408      bassa
87                                            Podologo   1,4  2021,2022   -36.363636      bassa
88                                            Podologo   5,8  2021,2022   -18.241042      bassa
89                                            Podologo  9,12  2021,2022   -28.275862      bassa
90   Terapista della Neuro e Psicomotricità dell'Et...   1,4  2019,2020   110.169492       alta
91   Terapista della Neuro e Psicomotricità dell'Et...   5,8  2019,2020     0.904977   costante
92   Terapista della Neuro e Psicomotricità dell'Et...  9,12  2019,2020    64.285714      media
93   Terapista della Neuro e Psicomotricità dell'Et...   1,4  2020,2021    32.258065   costante
94   Terapista della Neuro e Psicomotricità dell'Et...   5,8  2020,2021    29.147982   costante
95   Terapista della Neuro e Psicomotricità dell'Et...  9,12  2020,2021   -49.360614      bassa
96   Terapista della Neuro e Psicomotricità dell'Et...   1,4  2021,2022   -39.024390      bassa
97   Terapista della Neuro e Psicomotricità dell'Et...   5,8  2021,2022   -34.722222      bassa
98   Terapista della Neuro e Psicomotricità dell'Et...  9,12  2021,2022   -10.101010      bassa
99                 Tecnico Riabilitazione Psichiatrica   1,4  2019,2020   138.260870       alta
100                Tecnico Riabilitazione Psichiatrica   5,8  2019,2020    14.220183   costante
101                Tecnico Riabilitazione Psichiatrica  9,12  2019,2020    59.469697      media
102                Tecnico Riabilitazione Psichiatrica   1,4  2020,2021    32.481752   costante
103                Tecnico Riabilitazione Psichiatrica   5,8  2020,2021    27.309237   costante
104                Tecnico Riabilitazione Psichiatrica  9,12  2020,2021   -33.254157      bassa
105                Tecnico Riabilitazione Psichiatrica   1,4  2021,2022   -33.884298      bassa
106                Tecnico Riabilitazione Psichiatrica   5,8  2021,2022   -19.873817      bassa
107                Tecnico Riabilitazione Psichiatrica  9,12  2021,2022    -6.049822      bassa


"""


