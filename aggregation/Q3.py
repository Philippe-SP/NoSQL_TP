from pymongo import MongoClient

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
company_collection = db["company"]

# Requete d'aggregation:
# Q3: Identifiez l’entreprise ayant le plus grand nombre de taxis avec un 
#     niveau d’autonomie de 5.

pipeline = [
    {   # Récupération des taxis possédant un niveau d'autonomie de 5 grace à "$filter"
        "$project": {
            "name": 1,
            "city": 1,
            "taxis_with_5_autonomy": {
                "$filter": {
                    "input": "$taxis",  
                    "as": "taxi",
                    "cond": {"$eq": ["$$taxi.autonomy_level", 5]} 
                }
            }
        }
    },
    {
        "$project": {
            "name": 1,
            "city": 1,
            "taxi_autonomy_5_count": {"$size": "$taxis_with_5_autonomy"} # Récupération des entreprises selon les taxis avec 5 en autonomie grace à "$size"
        }
    },
    {
        "$sort": {"taxi_autonomy_5_count": -1} # Trie dans l'ordre décroissant
    },
    {
        "$limit": 1 # Récupère seulement le 1er resultat
    }
]

result = company_collection.aggregate(pipeline)

# Affichage du resultat
print(list(result))