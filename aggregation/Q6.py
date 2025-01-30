from pymongo import MongoClient

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]
timeseries_collection = db["timeseries"]

# Requete d'aggregation:
# Q6 : Les villes ayant les taxis ayant le plus tendances à s'arreter, par 
#      ordre croissant

pipeline = [
    {
        "$lookup": {
            "from": "timeseries", # collection de jointure
            "localField": "taxis.license_plate", # Champs de la collection d'aggregation
            "foreignField": "license_plate", # Champ de la collection de jointure
            "as": "stoppd_taxis" # Nom du resultat des deux collections
        }
    },
    {
        "$unwind": "$stoppd_taxis" # Décomposition du resultat de la jointure pour la rendre utilisable
    },
    {
        "$match": {"stoppd_taxis.status": "stopped"} # Récupération des taxis à l'arret
    },
    {
        "$group": {
            "_id": "$city",
            "stoppd_taxis": {"$sum": 1} # Compte le nombre de taxis stoppé pour chaque ville
        }
    },
    {
        "$sort": {"stoppd_taxis": 1} # classe les ville selon leur nombre de taxis stoppé dans l'ordre croissant
    }
]

result = company_collection.aggregate(pipeline)

# Affichage du resultat
for doc in result:
    print(doc)
