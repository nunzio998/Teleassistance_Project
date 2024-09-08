import pandas as pd
import io

def conta_televisite_per_mesi(df, anni, mesi, tipologia):
    """
    Conta il numero totale di televisite per una tipologia di professionista sanitario specificata,
    in un intervallo di mesi e anni.

    Args:
    df (pd.DataFrame): Il DataFrame contenente i dati mensili per ogni tipologia e anno.
    anni (list): Lista degli anni di interesse.
    mesi (list): Lista dei mesi di interesse.
    tipologia (str): La tipologia di professionista sanitario di cui calcolare le televisite.

    Returns:
    dict: Un dizionario con gli anni come chiavi e il totale delle televisite per quell'anno come valori.
    """
    # Filtrare il DataFrame per includere solo gli anni e i mesi specificati
    df_filtered = df[(df['anno'].isin(anni)) & (df['mese'].isin(mesi))]

    # Filtrare ulteriormente per la tipologia di professionista sanitario specificata
    df_professionista = df_filtered[df_filtered['tipologia_professionista_sanitario'].str.lower() == tipologia.lower()]

    # Calcolare la somma delle televisite per ciascun anno
    return {anno: df_professionista[df_professionista['anno'] == anno]['conteggio'].sum() for anno in anni}

def calcola_incremento_percentuale(df, anno1, anno2, mesi, tipologia):
    """
    Calcola l'incremento percentuale delle televisite tra due anni per una tipologia di professionista sanitario
    specificata in un intervallo di mesi.

    Args:
    df (pd.DataFrame): Il DataFrame contenente i dati mensili per ogni tipologia e anno.
    anno1 (int): Il primo anno per il confronto.
    anno2 (int): Il secondo anno per il confronto.
    mesi (list): Lista dei mesi di interesse.
    tipologia (str): La tipologia di professionista sanitario di cui calcolare l'incremento percentuale.

    Returns:
    float: L'incremento percentuale tra i due anni per la tipologia specificata.
    """
    # Ottenere i conteggi per gli anni specificati
    conteggi = conta_televisite_per_mesi(df, [anno1, anno2], mesi, tipologia)

    # Calcolare il conteggio per i due anni
    conteggio_anno1 = conteggi.get(anno1, 0)
    conteggio_anno2 = conteggi.get(anno2, 0)

    # Calcolare l'incremento percentuale
    incremento = ((conteggio_anno2 - conteggio_anno1) / conteggio_anno1) * 100 if conteggio_anno1 > 0 else float('inf')
    return incremento


def classifica_incremento(percentuale):
    """
    Classifica l'incremento in base alla percentuale.

    Args: percentuale (float): La percentuale di incremento da classificare.

    Returns:
    str: La classificazione dell'incremento ('alta', 'media', 'costante', 'bassa').
    """
    if percentuale > 100:
        return 'alta'
    elif 50 < percentuale <= 100:
        return 'media'
    elif 0 <= percentuale <= 50:
        return 'costante'
    elif percentuale < 0:
        return 'bassa'
    else:
        return 'media'  # Per valori tra 1% e 50%, assumiamo 'media'.


def salva_incremento_percentuale_per_intervalli(df, tipologie, intervalli_anni_mesi, output_file):
    """
    Calcola e salva l'incremento percentuale delle televisite per vari intervalli di mesi e anni,
    per ogni tipologia di professionista sanitario, in un file Parquet.

    Args:
    df (pd.DataFrame): Il DataFrame contenente i dati mensili per ogni tipologia e anno.
    tipologie (list): Lista delle tipologie di professionisti sanitari di interesse.
    intervalli_anni_mesi (list): Lista degli intervalli di anni e mesi per cui calcolare l'incremento percentuale.
    output_file (str): Il percorso del file Parquet in cui salvare i risultati.
    """
    risultati = []
    for tipologia in tipologie:
        for (anno1, anno2, mesi) in intervalli_anni_mesi:
            # Calcolare l'incremento percentuale
            incremento = calcola_incremento_percentuale(df, anno1, anno2, mesi, tipologia)
            classificazione = classifica_incremento(incremento)
            risultati.append({
                'tipologia': tipologia,
                'mesi': f"{mesi[0]},{mesi[-1]}",
                'anno': f"{anno1},{anno2}",
                'percentuale': incremento,
                'incremento': classificazione
            })

    # Creare un DataFrame con i risultati
    df_risultati = pd.DataFrame(risultati)

    # Trasforma il DataFrame in un buffer Parquet senza salvarlo su disco
    incremento_percentuale_compatto = io.BytesIO()
    df_risultati.to_parquet(incremento_percentuale_compatto, index=False)

    # Sposta il puntatore all'inizio del buffer
    incremento_percentuale_compatto.seek(0)

    # Leggi il buffer Parquet in un nuovo DataFrame
    df_incremento = pd.read_parquet(incremento_percentuale_compatto)

    # Espandi i valori di 'mesi' per coprire tutti i mesi nel range e prendi solo il secondo anno
    expanded_rows = []
    for _, row in df_incremento.iterrows():
        mesi_range = row['mesi'].split(',')
        for mese in range(int(mesi_range[0]), int(mesi_range[1]) + 1):
            anno = int(row['anno'].split(',')[1])  # Prendi solo il secondo anno
            expanded_row = row.copy()
            expanded_row['month'] = mese
            expanded_row['anno'] = anno
            expanded_rows.append(expanded_row)

    df_incremento_expanded = pd.DataFrame(expanded_rows)

    # Rinomina la colonna 'anno' in 'year'
    df_incremento_expanded.rename(columns={'anno': 'year'}, inplace=True)

    # Mantieni solo le colonne necessarie
    df_incremento_expanded = df_incremento_expanded[['tipologia', 'month', 'year', 'percentuale', 'incremento']]

    # Trasforma il DataFrame in un buffer Parquet senza salvarlo su disco
    incremento_percentuale_espanso = io.BytesIO()
    df_incremento_expanded.to_parquet(incremento_percentuale_espanso, index=False)

    # Sposta il puntatore all'inizio del buffer
    incremento_percentuale_espanso.seek(0)

    # Leggi il buffer Parquet in un nuovo DataFrame
    df_incremento_percentuale_espanso = pd.read_parquet(incremento_percentuale_espanso)

    # Elimina la colonna 'index' se esiste
    if 'index' in df_incremento_percentuale_espanso.columns:
        df_incremento_percentuale_espanso.drop(columns=['index'], inplace=True)

    # Salva il DataFrame espanso in un file Parquet
    df_incremento_percentuale_espanso.to_parquet(output_file, index=False)


def extract_colum_increment(df,k):
    # Rinomina le colonne per facilitare l'unione
    k.rename(columns={'tipologia': 'tipologia_professionista_sanitario', 'month': 'mesi'}, inplace=True)

    # Unisci i DataFrame sulle colonne 'tipologia_professionista_sanitario', 'year', e 'mesi'
    df_merged = pd.merge(df, k[['tipologia_professionista_sanitario', 'year', 'mesi', 'incremento']],
                         left_on=['tipologia_professionista_sanitario', 'year', 'month'],
                         right_on=['tipologia_professionista_sanitario', 'year', 'mesi'],
                         how='left')

    # Rimuovi la colonna 'mesi' che è stata aggiunta durante l'unione
    df_merged.drop(columns=['mesi'], inplace=True)

    return df_merged

def incremento(df):

    # Carica il file df_aggregato.parquet
    file_path = 'datasets/df_aggregato.parquet'
    df_aggregato = pd.read_parquet(file_path)

    print("Calcolo la variabile incremento...")

    # Ottieni tutte le tipologie di professionisti sanitari presenti nel dataset
    tipologie = df_aggregato['tipologia_professionista_sanitario'].unique()

    # Definisci gli intervalli di anni e mesi di interesse
    intervalli_anni_mesi = [
        (2019, 2020, [1, 2, 3, 4]),
        (2019, 2020, [5, 6, 7, 8]),
        (2019, 2020, [9, 10, 11, 12]),
        (2020, 2021, [1, 2, 3, 4]),
        (2020, 2021, [5, 6, 7, 8]),
        (2020, 2021, [9, 10, 11, 12]),
        (2021, 2022, [1, 2, 3, 4]),
        (2021, 2022, [5, 6, 7, 8]),
        (2021, 2022, [9, 10, 11, 12])
    ]

    # Salva l'incremento percentuale per i vari intervalli di mesi e anni per ogni tipologia di professionista sanitario in un file Parquet
    output_file = 'datasets/incremento_percentuale.parquet'
    salva_incremento_percentuale_per_intervalli(df_aggregato, tipologie, intervalli_anni_mesi, output_file)
    L = pd.read_parquet('datasets/incremento_percentuale.parquet')
    df = extract_colum_increment(df, L)

    return df


























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


