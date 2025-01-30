from pymongo import MongoClient

# Récupération de la base de données et des collections
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]
timeseries_collection = db["timeseries"]

# Requete d'aggregation:
# Q5 : Déterminez les zones (basées sur des quadrants de coordonnées 
#      GPS) où les taxis s’arrêtent le plus fréquemment dans une ville donnée.

paris_zone = [[2.3522, 48.8566], [2.3522, 49.0000], [2.4000, 49.0000], [2.4000, 48.8566], [2.3522, 48.8566]] # coordonnées de Paris (environ)

pipeline = [
    {
        "$project": {
            "license_plate": 1,
            "lat": 1,
            "lon": 1,
            "status": 1,
            "location": { # Création d'un champs location pour l'aggrégation
                "$let": { # Definition de variables locales pour la latitude et longitude
                    "vars": {
                        "longitude": "$lon",
                        "latitude": "$lat"
                    },
                    "in": [ # Ajout des variables dans un tableau
                        "$$longitude",
                        "$$latitude"
                    ]
                }
            }
        }
    },
    {
        "$match": { # Récupération des taxis présent dans la zone de Paris et stoppé
            "status": "stopped",
            "location": {
                "$geoWithin": {
                    "$geometry": {
                        "type": "Polygon",
                        "coordinates": [paris_zone]
                    }
                }
            }
        }
    }
]

result = timeseries_collection.aggregate(pipeline)

# Affichage du resultat
for doc in result:
    print(doc)