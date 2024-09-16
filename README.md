<div align="center">
  <h1>Sviluppo del Clustering Supervisionato per lo studio dell'incremento delle Teleassistenze in Italia </h1>
</div>

***
## Descrizione del Progetto

L'obiettivo del progetto è profilare i pazienti presenti nel dataset `challenge_campus_biomedico_2024.parquet` tenendo conto del loro contributo all'aumento del servizio di teleassistenza.   
Per fare questo, si identifica una variabile target `incremento_teleassistenza`, che viene considerata come feature principale per guidare il clustering; per questo motivo, parliamo di *"Clustering Supervisionato"*.  
Il clustering viene svolto utilizzando l'algoritmo K-Means, che sfrutta le caratteristiche dei pazienti e la variabile target per suddividere i dati nei cluster.   
In seguito, vengono analizzate le differenze tra i pazienti dei vari gruppi di incremento, per comprendere quali caratteristiche influenzano l'aumento delle teleassistenze. Così facendo, è possibile identificare gruppi di pazienti con schemi comuni o comportamenti simili che influenzano maggiormente l'andamento delle teleassistenze.

## Struttura del Progetto
Il progetto è strutturato in diverse fasi:

**1.  Pre-Processing dei Dati**:

La fase di Pre-Processing comprende:
- Data Cleaning
- Feauture Selection

**2. Feature Extraction**:

La fase di Feature Extraction comprende:
- Estrazione di nuove feature
- Estrazione della variabile target `incremento_teleassistenza`

**3. Data Transformation**:

La fase di Data Transformation comprende:
- Encoding delle feature

**4. Clustering Execution**:

La fase di Clustering Execution comprende:
- Esecuzione dell'algoritmo di clustering K-Means
- Creazione delle metriche per la valutazione del clustering (`Metrica di Purezza`, `Metrica di Silhouette` e `Metrica finale`)
- Creazione di grafici per l'identificazione di pattern e feature rilevanti


## Installazione e Setup
**1. Clona la repository:**

```bash
git clone https://github.com/username/nome_repo.git
cd nome_repo
```

**2. Installa le dipendenze:**
```bash
pip install -r requirements.txt
```
## Esecuzione del Codice
```bash
python run.py
```
