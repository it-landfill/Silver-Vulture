# Silver-Vulture

## Installazione
Al fine di poter utilizzare Silver Vulture, è necessario un setup iniziale. 

Dopo essersi assicurati di avere un account con credito su GCP, bisogna:
- Creare un bucket ed eseguire l’update del dataset con i rating;
- Esportare le credenziali seguendo questa guida [2], salvando il file nella stessa cartella dello script Python (“/scripts”);
- Creare un venv e installare le librerie indicate nel file “requirements.txt”;
- Modificare il file di configurazione nella cartella “/scripts” in base alle proprie necessità;
- Se si vuole eseguire l’evaluation, è necessario far eseguire prima il ranking


Terminata la configurazione, l’utente può eseguire “launchRemoteJob.py”, il quale:
- Carica su un bucket specificato la versione più recente del .jar di Silver Vulture e fa del cleanup; 
- Esegue il provisioning di un cluster composto da un master e 5 worker;
- Definisce un job, inserendo i parametri d’avvio di Silver Vulture in base al tipo di workflow selezionato;
- Lancia il job precedentemente definito, rimanendo in attesa per il completamento;
- Elimina il cluster, in modo da evitare sperpero di risorse non necessario;
- Scarica dal bucket il file di output, in formato CSV, ed interroga myanimelist per ottenere dettagli sugli anime indicati, per poi presentare il risultato all’utente.

## Configurazione
Il file di configurazione del launcher è in formato JSON, e contiene al suo interno i seguenti campi:
- "cred_path": il percorso in cui sono salvate le credenziali di GCP;
- "jar_name": il percorso in cui è contenuto il JAR;
- "is_running_locally": attiva/disattiva le capacità di clustering di spark;
- "use_mllib": attiva/disattiva l'algoritmo ALS per il suggerimento degli anime;
- "regen_ranking": rigenera i file di ranking;
- "run_evaluation": esegue la valutazione del MAE (controllare la console del cluster per osservare il risultato);
- "user_id": l'id su cui fare la predizione;
- "threshold": soglia minima di voto;
- "number_of_results: numero di risultati da presentare;
- "bucket_name": nome del bucket su GCP;
- "project_id": id del progetto su GCP;
- "region": regione del cluster;
- "cluster_name": nome del cluster;
Per eseguire l'evaluation, è importante aver prima eseguito un job di ranking.
