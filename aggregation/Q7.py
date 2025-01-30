from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]
timeseries_collection = db["timeseries"]

# Requete d'aggregation:
# Q7 : Identifiez les taxis actuellement à l'arret et créez en une vue. (exemple dans le pdf)

now = datetime.now(timezone.utc) # Variable correspondant à la date et heure actuelle

view_pipeline = [
    {
        "$lookup": {
            "from": "timeseries", # collection de jointure
            "localField": "taxis.license_plate", # Champs de la collection d'aggregation
            "foreignField": "license_plate", # Champ de la collection de jointure
            "as": "timeseries_info" # Nom du resultat des deux collections
        }
    },
    {
        "$unwind": "$timeseries_info" # Décomposition du resultat de la jointure pour la rendre utilisable
    },
    {
        "$sort": {"timeseries_info.timestamp": -1} # Trie dans l'ordre décroissant pour avoir le status le plus récent en 1er
    },
    {   # Regroupe tout les éléments nécéssaires à la création de la vue
        "$group": {
            "_id": "$timeseries_info.license_plate", # Récupération de la plaque d'immatriculation du taxi
            "last_status": { "$first": "$timeseries_info.status" },  # Récupération du status du taxi
            "last_timestamp": { "$first": "$timeseries_info.timestamp" },  # Récupération du timestamp
            "location": {"$first": "$taxis.location"}, # Récupération de la localisation du taxi
            "company_name": {"$first": "$name"}, # Récupération du nom de l'entreprise
            "company_city": {"$first": "$city"}, # Récupération de la ville de l'entreprise
            "company_fleet_size": {"$first": "$fleet_size"} # Récupération de la taille de la flotte de l'entreprise
        }
    }
]

# Création de la vue
db.create_collection("taxis_currently_stopped", viewOn = "company", pipeline = view_pipeline)

# affichage de la vue
db.taxis_currently_stopped.find()
