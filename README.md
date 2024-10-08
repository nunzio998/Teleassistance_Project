<div align="center">
  <h1>Sviluppo del Clustering Supervisionato per lo studio dell'incremento delle Teleassistenze in Italia </h1>
</div>

***
## Descrizione del Progetto

L'obiettivo del progetto è profilare i pazienti presenti nel dataset `challenge_campus_biomedico_2024.parquet` tenendo conto del loro contributo all'aumento del servizio di teleassistenza in Italia.   
Per fare questo, si identifica una variabile target `Incremento_teleassistenza`, che viene considerata come feature principale per guidare il Clustering; per questo motivo, parliamo di *Clustering Supervisionato*.  
Il Clustering viene svolto utilizzando l'algoritmo `K-Means`, che sfrutta sia le caratteristiche dei pazienti che la variabile target per suddividere i dati nei cluster.   
In seguito, vengono analizzate le differenze tra i pazienti dei vari gruppi di incremento, per comprendere quali caratteristiche influenzano l'aumento delle teleassistenze. Così facendo, è possibile identificare gruppi di pazienti con schemi comuni o comportamenti simili che influenzano maggiormente l'andamento delle teleassistenze.

## Struttura del Progetto
Il progetto è organizzato come segue:

```
Teleassistance_Project/
├── documents/
│   └── challenge_campus_biomedico.pdf
├── src/
│   ├── clustering/
│   │   ├── clustering_analyzer.py
│   │   ├── clustering_execution.py
│   │   └── clustering_metrics.py
│   │
│   ├── data_prep/
│   │   ├── data_cleaning.py
│   │   └── features_selection.py
│   │
│   ├── data_transformation/
│   │   └── data_transformation.py
│   │
│   ├── datasets/
│   │   ├── challenge_campus_biomedico_2024.parquet
│   │   ├── Codici-statistici-e-denominazioni-al-30_06_2024.xlsx
│   │   ├── df_aggregato.parquet
│   │   └── df_incremento_percentuale_esteso.parquet
│   │
│   ├── feature_extraction/
│   │   ├── extract_increment.py
│   │   └── features_extraction.py
│   │
│   ├── graphs/
│   ├── month_dataset/
│   └── run.py
│
├── README.md
├── requirements.txt
└── results.txt
```

Le fasi in cui il progetto è strutturato sono le seguenti:

### Preprocessing dei Dati

La fase di Preprocessing comprende:
- Data Cleaning
- Feautures Selection

#### Data Cleaning
Durante la fase di Data Cleaning vengono eseguite le seguenti operazioni per la pulizia del dataset:
- **Imputazione dei valori mancanti**: Viene effettuato un check per verificare quali sono le features che contengono valori mancanti; successivamente, si
passa all'imputazione degli stessi. Le features per le quali viene svolto tale processo sono: 'comune_residenza', 'codice_provincia_residenza', 'codice_provincia_erogazione',
'ora_inizio_erogazione' e 'ora_fine_erogazione'.

- **Rimozione dei campioni con 'data_disdetta' non nullo**: Questi campioni vengono rimossi in quanto relativi a televisite che non sono avvenute poiché disdette.

- **Identificazione e rimozione degli outliers**: Questa operazione avviene per le seguenti features: 'data_nascita', 'data_contatto', 'data_erogazione', 'ora_inizio_erogazione' e 'ora_fine_erogazione'.
  
- **Gestione dei dati rumorosi**: Questa operazione avviene per le seguenti features: 'data_nascita', 'data_contatto', 'data_erogazione', 'ora_inizio_erogazione' e 'ora_fine_erogazione'.

- **Rimozione dei duplicati**: Se presenti, vengono rimossi i campioni duplicati.
  
- **Ordinamento delle date di erogazione**: I campioni vengono ordinati in base alla data di erogazione del servizio.


#### Feature Selection
Durante la fase di Features Selection vengono eseguite le seguenti operazioni:

- **Analisi della correlazione univoca**: Viene eseguita un'analisi per controllare se due features hanno tra loro correlazione univoca. In caso affermativo
una delle due viene rimossa. Questa analisi viene effettuata sulle seguenti coppie di features:<br>
  - 'codice_provincia_residenza', 'provincia_residenza'
  - 'codice_provincia_erogazione', 'provincia_erogazione'
  - 'codice_regione_residenza', 'regione_residenza'
  - 'codice_asl_residenza', 'asl_residenza'
  - 'codice_comune_residenza', 'comune_residenza'
  - 'codice_descrizione_attivita', 'descrizione_attivita'
  - 'codice_regione_erogazione', 'regione_erogazione'
  - 'codice_asl_erogazione', 'asl_erogazione'
  - 'codice_struttura_erogazione', 'struttura_erogazione'
  - 'codice_tipologia_struttura_erogazione', 'tipologia_struttura_erogazione'
  - 'codice_tipologia_professionista_sanitario', 'tipologia_professionista_sanitario'
  
- **Rimozione 'data_disdetta'**: Tale feature viene rimossa poiché dopo il processo di Data Cleaning presenterà solo valori mancanti.
  
- **Rimozione 'id_prenotazione'**: La feature viene rimossa in quanto considerata non significativa ai fini delle analisi svolte sull'andamento delle televisite.
  
- **Check 'regione_residenza' & 'reione_erogazione'**: Viene effettuato un check sulla coppia di features per verificare se per ogni campione i loro valori corrispondono. In caso affermarivo, viene rimossa la feature 'regione_erogazione'.
  
- **Check 'tipologia_servizio'**: Viene effettuato un check su tale feature per verificare se il valore è il medesimo per ogni campione. In caso affermativo, la feature stessa viene rimossa.
Tale verifica è stata ritenuta necessaria in quanto, dopo una prima visione del dataset, si è notato che per tutti i campioni visionati il valore corrisponde sempre a 'Teleassistenza'.

### Feature Extraction

La fase di Feature Extraction comprende:
- **Estrazione di nuove feature**:
  - Dalla feature "data_nascita" viene estratta la nuova feature "età_paziente".
  - Dalle feature "ora_inizio_erogazione" e "ora_fine_erogazione" viene estratta la nuova feature "durata_televisita".
  - Dalla feature "data_erogazione" vengono estratte le feature "anno" e "mese".

- **Estrazione della variabile target `Incremento_teleassistenza`**:

La feature viene estratta seguendo diverse fasi:
  - Calcolo della richiesta di ogni professionista sanitario per un intervallo temporale di 6 mesi.
  - Calcolo dell'incremento percentuale per semestri di anni successivi.
  - Creazione di una nuova feature "Incremento_teleassistenza", che può assumere valori alto, medio, basso e costante.

### Data Transformation

La fase di Data Transformation comprende:
- **Encoding delle feature**: Le feature categoriche vengono convertite in feature numeriche tramite codifica *Label Encoding*.
- **Dimensionality Reduction**: Il numero di feature viene ridotto utilizzando la tecnica di Dimensionality Reduction *TruncatedSVD*. Queste tecnica, diversamente dalla PCA, può essere applicata ai dati sparsi senza la necessità di centrare i dati.
La fase di Dimensionality Reduction è fondamentale per una corretta esecuzione dell'algoritmo di Clustering.

### Clustering Execution

La fase di Clustering Execution comprende:
- Calcolo del numero ottimale di Clustering grazie alla tecnica dell'**Elbow Method**. Questo metodo aiuta a determinare il numero ottimale di cluster nel K-Means tracciando l'inerzia (varianza interna ai cluster) rispetto al numero di cluster e identificando il punto in cui il miglioramento si riduce drasticamente, formando un "gomito".
  
- Esecuzione dell'algoritmo di Clustering **K-Means**. Il K-Means suddivide i dati in K cluster iniziando con K centroidi scelti casualmente. A ogni iterazione, assegna i punti al centroide più vicino e ricalcola i centroidi come la media dei punti nel cluster, ripetendo il processo fino a convergenza, quando le assegnazioni non cambiano più.
  
- Creazione delle **Metriche** per la valutazione del Clustering:
  - `Metrica di Purezza`: La Purezza del clustering misura la qualità di un clustering calcolando la proporzione di campioni correttamente classificati all'interno di ciascun cluster.
  - `Metrica di Silhouette`: L'Indice di Silhouette valuta la qualità del clustering misurando quanto i campioni siano vicini ai punti del loro stesso cluster rispetto ai punti di altri cluster.
  - `Metrica finale`: La metrica finale viene calcolata come la differenza fra la media delle due metriche normalizzate e un termine di penalità pari a 0.05 volte il numero di cluster.
- Creazione di grafici per l'identificazione di pattern e di feature rilevanti.


## Installazione e Setup
**1. Clona la repository:**

```bash
git clone https://github.com/nunzio998/Teleassistance_Project.git
cd Teleassistance_Project
```

**2. Crea un ambiente virtuale**:

Per ambiente MacOS:
```bash
python3 -m venv venv
source venv/bin/activate
```

Per ambiente Windows:
```bash
python -m venv venv
.\venv\Scripts\activate
```

**3. Installa le dipendenze:**

```bash
pip install --upgrade pip
pip install -r requirements.txt

```
## Esecuzione del Codice
```bash
python run.py
```
