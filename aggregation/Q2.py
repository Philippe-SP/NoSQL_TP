from pymongo import MongoClient

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]
timeseries_collection = db["timeseries"]

# Requete d'aggregation:
# Q2: Le taxi qui s'est arrêté le plus de fois (avec les informations de son 
#     entreprise)

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
        "$match": {"real_time_data.status": "stopped"}
    },
    {
        "$group": {
            "_id": "$real_time_data.license_plate", # Regroupement des taxis en fonction de leur plaque d'immatriculation
            "taxi_stopped_count": {"$sum": 1} # Nombre de fois où le taxi s'est arrété
        }
    },
    {
        "$sort": {"taxi_stopped_count": -1} # Ordre décroissant pour avoir le taxi le plus souvent arreté en 1er
    },
    {
        "$limit": 1 # Récupération seulement du premier élément
    }
]

result = company_collection.aggregate(pipeline)
# Affichage du resultat
print(list(result))
