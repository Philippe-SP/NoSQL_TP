from pymongo import MongoClient
import json
import pymongo
import paho.mqtt.client as mqtt

# ----------------------------------------------- Initialisation de la base de donnée ------------------------------------------------- #
mongodb_client = MongoClient("localhost", 27017)
db = mongodb_client["AutoDrive_Pinheiro"]

# Création de la collection timeseries
timeseries_collection = db["timeseries"]

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
        "taxis": {
            "bsonType": "array",
            "description": "Taxis de l'entreprise"
        },
    }
}

# Création de la collection company
try:
    company_collection = db.create_collection("company", validator = {"$jsonSchema": company_schema})
    print("Collection 'company' créée avec succès avec un schéma de validation.")

    # Ajout des données via le fichier "taxis.json"
    with open("data/taxis.json") as f:
        data_file = json.load(f)

    try:
        if isinstance(data_file, list):
            result = company_collection.insert_many(data_file)
            print(f"Insertion réussie : {len(result.inserted_ids)} documents ajoutés.")
        else:
            print("Les données ne sont pas au format attendu (liste d'objets JSON).")
    # Erreur d'insertion
    except pymongo.errors.BulkWriteError as e:
        print("Erreur lors de l'insertion :", e.details)
# Erreur de création de la collection
except pymongo.errors.CollectionInvalid:
    print("La collection 'company' existe déjà.")

# ------------------------------------------------- Initialisation du server MQTT ------------------------------------------------------- #
# Callback quand la connexion au broker est établie
def on_connect(mqtt_client, userdata, flags, rc):
    if rc == 0:
        print("Connecté au broker MQTT !")
        # Connection au topic
        mqtt_client.subscribe("autonomous_taxis/gps")
    else:
        print(f"Erreur de connexion : {rc}")

# Callback quand un message est reçu
def on_message(mqtt_client, userdata, msg):
    print(f"Message reçu sur le topic {msg.topic}: {msg.payload.decode()}")
    
    # Convertion du message binaire en chaîne de caractères
    message_str = msg.payload.decode()

    # Convertion de la chaîne de caractères en document JSON
    message_json = json.loads(message_str)
    timeseries_collection.insert_one(message_json)

# Configuration du client MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connexion au broker MQTT
mqtt_client.connect("broker.hivemq.com", 1883, 60)

# Boucle infinie pour attendre les messages
mqtt_client.loop_forever()