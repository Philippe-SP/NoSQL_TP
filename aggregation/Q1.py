from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]
timeseries_collection = db["timeseries"]

# Requete d'aggregation:
#       Q1 : Comptez, par ville, le nombre d'entrées représentant les taxis en
#            mouvement (avec des données récentes, par exemple dans la dernière
#            heure).

one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1) # Variable correspondant à la dernière heure

pipeline = [
    {
        "$lookup": {
            "from": "timeseries", # collection de jointure
            "localField": "taxis.license_plate", # Champs de la collection d'aggregation
            "foreignField": "license_plate", # Champ de la collection de jointure
            "as": "real_time_data" # Nom du resultat des deux collections
        }
    },
    {
        "$unwind": "$real_time_data" # Décomposition du resultat de la jointure pour la rendre utilisable
    },
    {
        "$match": {
            "$expr": {
                "$gte": [
                    {"$dateFromString": {"dateString": "$real_time_data.timestamp"}}, # Filtre pour récupérer les valeurs de la dernière heure en convertissant timestamp en datetime
                    one_hour_ago
                ]
            }
        }
    },
    {
        "$match": {"real_time_data.status": "moving"} # Récupération des taxis en mouvement
    },
    {
        "$group": {
            "_id": "$city",
            "moving_taxis_count": {"$sum": 1} # Regroupement du nombre de taxis en mouvement
        }
    }
]

result = company_collection.aggregate(pipeline)

# Affichage des résultats
for doc in result:
    print(doc)