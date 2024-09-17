<div align="center">
  <h1>Sviluppo del Clustering Supervisionato per lo studio dell'incremento delle Teleassistenze in Italia </h1>
</div>

***
## Descrizione del Progetto

L'obiettivo del progetto è profilare i pazienti presenti nel dataset `challenge_campus_biomedico_2024.parquet` tenendo conto del loro contributo all'aumento del servizio di teleassistenza.   
Per fare questo, si identifica una variabile target `incremento_teleassistenza`, che viene considerata come feature principale per guidare il Clustering; per questo motivo, parliamo di *Clustering Supervisionato*.  
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

### Pre-Processing dei Dati

La fase di Pre-Processing comprende:
- Data Cleaning
- Feauture Selection

#### Data Cleaning
Durante la fase di data cleaning vengono eseguite le seguenti operazioni per la pulizia del dataset:
- **Imputazione dei valori mancanti**: viene effettuato un check per verificare quali sono le feature che contengono valori mancanti, successivamente si
passa all'imputazione degli stessi. Le features per le quali viene svolto tale processo sono: 'comune_residenza', 'codice_provincia_residenza', 'codice_provincia_erogazione',
'ora_inizio_erogazione' e 'ora_fine_erogazione'
- **Rimozione dei campioni con 'data_disdetta' non nullo**: questi campioni vengono rimossi in quanto relativi a televisite che non sono avvenute poiché disdette.
- **Identificazione e rimozione degli outliers**: questa operazione avviene per le seguenti features: 'data_nascita', 'data_contatto', 'data_erogazione', 'ora_inizio_erogazione' e 'ora_fine_erogazione'
- **Gestione dei dati rumorosi**: questa operazione avviene per le seguenti features: 'data_nascita', 'data_contatto', 'data_erogazione', 'ora_inizio_erogazione' e 'ora_fine_erogazione'
- **Rimozione dei duplicati**: se presenti, vengono rimossi i campioni duplicati.
- **Ordinamento delle date di erogazione**: I campioni vengono ordinati in base alla data di erogazione del servizio.

#### Feature Selection
Durante la fase di features selection vengono eseguite le seguenti operazioni:
- **Analisi della correlazione univoca**: viene eseguita un'analisi per controllare se due features hanno tra loro correlazione univoca. In caso affermativo
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
- **Rimozione 'data_disdetta'**: tale features viene rimossa poiché dopo il processo di data cleaning presenterà solo valori mancanti.
- **Rimozione 'id_prenotazione'**: la feature viene rimossa in quanto considerata non significativa ai fini delle analisi svolte sull'andamento delle televisite.
- **Check 'regione_residenza' & 'reione_erogazione'**: viene effettuato un check sulla coppia di feature per verificare se per ogni campione i loro valori corrispondono. In caso affermarivo viene rimossa l afeature 'regione_erogazione'.
- **Check 'tipologia_servizio'**: viene effettuato un check su tale features per verificare se il valore è il medesimo per ogni campione. In caso affermativo la feature stessa viene rimossa.
Tale verifica è stata ritenuta necessaria in quanto, dopo una prima visione del dataset, si è notato che per tutti i campioni visionati il valore corrisponde sempre a 'Teleassistenza'.

### Feature Extraction

La fase di Feature Extraction comprende:
- Estrazione di nuove feature
- Estrazione della variabile target `incremento_teleassistenza`

### Data Transformation

La fase di Data Transformation comprende:
- Encoding delle feature

### Clustering Execution

La fase di Clustering Execution comprende:
- Esecuzione dell'algoritmo di Clustering **K-Means**
- Creazione delle metriche per la valutazione del Clustering: `Metrica di Purezza`, `Metrica di Silhouette` e `Metrica finale`
- Creazione di grafici per l'identificazione di pattern e feature rilevanti


## Installazione e Setup
**1. Clona la repository:**

```bash
git clone https://github.com/username/nome_repo.git
cd nome_repo
```

**2. Crea un ambiente virtuale**:
```bash
python -m venv venv
.\venv\Scripts\activate
```

**3. Installa le dipendenze:**
```bash
pip install -r requirements.txt
```
## Esecuzione del Codice
```bash
python run.py
```
