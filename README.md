# NoSQL_TP

Contenu de l'archive:
    - Dossier aggregation:                Scripts python des différentes requetes d'aggregation séparée pour pouvoir les tester séparément.
    - Dossier capture_ecran_aggregation:  Les captures d'écran du résultat des mes requetes d'aggregation testée.
    - Dossier data:                       Le fichier JSON contenant les données a jouté en base de données.
    - Fichier main.py:                    Script python permettant de récupérer les données MQTT et la création de la bdd.


Package nécéssaire pour tester mon travail:
    - pymongo
    - datetime, timedelta, timezone
    - json
    - paho.mqtt.client


Si vous avez besoins de mon environnement virtuel, vous pourrez le trouver sur le repos github suivant:
    https://github.com/Philippe-SP/NoSQL_TP.git


Etapes de mon travail:
    1. J'ai créer mon environnement virtuel puis ma base de données depuis le script python "main.py"
    2. J'ai ensuite ajouté les données via le fichier JSON que vous nous avez fournis
    3. J'ai ajouté a mon script "main.py" les fonctions nécéssaire pour récupérer les données MQTT envoyé via votre script
    4. J'ai ensuite effectué mes requetes d'aggregations dans des fichiers séparé et testé celles-ci dans jupyter

Pour effectuer ce travail je me suis aider de vos cours, des documentations, ainsi que de chatGPT. Pour l'utilisation de celui-ci j'ai cherché a comprendre chacune des aides qu'il m'a fournis et ai commenté mon code entièrement moi meme pour décrire ma compréhensions des fonctions et requetes que nous n'avons pas vu en cours.