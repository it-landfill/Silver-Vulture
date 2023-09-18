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
