from pymongo import MongoClient

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]

# Requete d'aggregation:
# Q4 : Pour chaque entreprise, calculez la moyenne d’âge de leur flotte 
#      (basez-vous sur year_of_production).

pipeline = [
    {
        "$unwind": "$taxis"  # Décompose les documents de la flotte de taxis
    },
    {
        "$group": {
            "_id": "$name",
            "fleet_age_average": {"$avg": "$taxis.year_of_production"} # Regroupe les entreprises avec leur moyenne d'age de leur flotte
        }
    }
]

result = company_collection.aggregate(pipeline)

# Affichage du resultat
print(list(result))