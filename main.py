from pymongo import MongoClient
import json
import pymongo

# ----- Initialisation de la base données ----- #
client = MongoClient("localhost", 27017)
db = client["AutoDrive"]

# Création du schéma de validation
company_schema = {
    "bsonType": "object",
    "required": ["name", "city", "address", "founded_year", "fleet_size", "taxis"],
    "properties": {
        "name": {
            "bsonType": "string",
            "description": "Le nom de l'entreprise"
        },
        "city": {
            "bsonType": "string",
            "description": "La ville où est située l'entreprise"
        },
        "address": {
            "bsonType": "string",
            "description": "Adresse exacte de l'entreprise"
        },
        "founded_year": {
            "bsonType": "int",
            "description": "L'année de création de l'entreprise"
        },
            "fleet_size": {
            "bsonType": "int",
            "description": "Nombre de véhicules de l'entreprise"
        },
        "contact_email": {
            "bsonType": "string",
            "description": "Email de l'entreprise"
        },
        "contact_phone": {
            "bsonType": "string",
            "description": "N° de téléphone de l'entreprise"
        },
        "Taxis": {
            "bsonType": "array",
            "description": "Taxis de l'entreprise"
        },
    }
}

# Création des collection
try:
    company_collection = db.create_collection("company", validator = {"$jsonSchema": company_schema})
    print("Collection 'company' créée avec succès avec un schéma de validation.")
except pymongo.errors.CollectionInvalid:
    print("La collection 'company' existe déjà.")

# Ajout des données via le fichier "taxis.json"
with open("data/taxis.json") as f:
    data_file = json.load(f)

try:
    if isinstance(data_file, list):
        result = company_collection.insert_many(data_file)
        print(f"Insertion réussie : {len(result.inserted_ids)} documents ajoutés.")
    else:
        print("Les données ne sont pas au format attendu (liste d'objets JSON).")
except pymongo.errors.BulkWriteError as e:
    print("Erreur lors de l'insertion :", e.details)