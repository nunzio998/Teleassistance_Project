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
