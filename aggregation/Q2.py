from pymongo import MongoClient

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]
timeseries_collection = db["timeseries"]

# Requete d'aggregation:
# Q2: Le taxi qui s'est arrêté le plus de fois (avec les informations de son 
#     entreprise)

timeseries_pipeline = [
    {
        "$match": {"status": "stopped"}
    },
    {
        "$group": {
            "_id": "license_plate", # Regroupement des taxis en fonction de leur plaque d'immatriculation
            "taxi_stopped_count": {"$sum": 1} # Nombre de fois où le taxi s'est arrété
        }
    },
    {
        "$sort": {"taxi_stopped_count": -1} # Ordre décroissant pour avoir le taxi le plus souvent arreté en 1er
    },
    {
        "$limit": 1 # Récupération seulement du premier élément
    },
]

timeseries_result = list(timeseries_collection.aggregate(timeseries_pipeline))

company_pipeline = [
    {
        "$match": {"taxis.license_plate": timeseries_result[0]["_id"]} # Récupération des données de l'entreprise du taxi à la plaque "_id"
    }
]

company_result = company_collection.aggregate(company_pipeline)

# Affichage du resultat
print(list(company_result))
