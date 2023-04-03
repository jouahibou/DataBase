from pymongo import MongoClient
from pprint import pprint
#A Connection à mongoDB
client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "admin",
    password = "pass",
    
)
db = client["sample"]
collection = db["books"]

# B Liste des bases de données disponibles
print(client.list_database_names())

# C Liste des collections disponibles 
print(db.list_collection_names())

# D Afficcher un des documents 
document = collection.find_one()
pprint(document)

# E Afficher le nombre de documents
nombre_document = collection.count_documents({})
print(nombre_document)

## Exploration de la base 

# A Afficher le nombre de livres avec plus de 400 pages
nb_livres_plus_de_400_pages = collection.count_documents({"pageCount": {"$gt": 400}})
print("Nombre de livres avec plus de 400 pages :", nb_livres_plus_de_400_pages)

# Afficher le nombre de livres avec plus de 400 pages et qui sont publiés
nb_livres_plus_de_400_pages_publies = collection.count_documents({"pageCount": {"$gt": 400}, "status": "PUBLISH"})
print("Nombre de livres avec plus de 400 pages et qui sont publiés :", nb_livres_plus_de_400_pages_publies)

# B 
#Afficher le nombre de livres ayant le mot-clé Android dans leur description (brève ou longue).
nb_livres_android = collection.count_documents({"$or": [{"shortDescription": {"$regex": ".*Android.*"}}, {"longDescription": {"$regex": ".*Android.*"}}]})
# (c) Afficher les 2 listes des categories distinctes
categories = collection.distinct('categories')
print("Liste des catégories distinctes :", categories)

# (d) Afficher le nombre de livres qui contiennent des noms de langages suivant dans leur description longue : Python, Java, C++, Scala
nb_livres_langages = collection.count_documents({
    "longDescription": {
        "$regex": "Python|Java|C\+\+|Scala",
        "$options": "i"
    }
})
print("Nombre de livres contenant les langages Python, Java, C++ ou Scala :", nb_livres_langages)

# (e) Afficher diverses informations statistiques sur notre bases de données : nombre maximal, minimal, et moyen de pages par livre
pipeline = [
    {"$group": {
        "_id": None,
        "max_pages": {"$max": "$pageCount"},
        "min_pages": {"$min": "$pageCount"},
        "avg_pages": {"$avg": "$pageCount"}
    }}
]
result = list(collection.aggregate(pipeline))
if len(result) > 0:
    stats = result[0]
    print("Nombre maximal de pages par livre :", stats['max_pages'])
    print("Nombre minimal de pages par livre :", stats['min_pages'])
    print("Nombre moyen de pages par livre :", round(stats['avg_pages'], 2))
    
#(f) Créer une nouvelle colonne à partir de la liste des auteurs :    
    
pipeline = [
    # Filtrer les livres avec 2 auteurs
    { "$match": { "authors": { "$size": 2 } } },

    # Créer deux colonnes pour les deux auteurs
    { "$addFields": {
        "author1": { "$arrayElemAt": [ "$authors", 0 ] },
        "author2": { "$arrayElemAt": [ "$authors", 1 ] }
    } }
]

# Appliquer la pipeline sur la collection
result = (db.books.aggregate(pipeline))

# Afficher les 5 premiers résultats
i=0
for doc in result:
    i+=1
    if i<5:
      pprint(doc)
    else:
      pass  
    
    
# g Créer une colonne contenant le nom du premier auteur, puis agréger selon cette colonne 
#pour obtenir le nombre d'articles pour chaque premier auteur    
    
pipeline = [
    # Créer une colonne pour le premier auteur
    { "$addFields": { "first_author": { "$arrayElemAt": [ "$authors", 0 ] } } },

    # Regrouper par premier auteur et compter le nombre de livres
    { "$group": {
        "_id": "$first_author",
        "count": { "$sum": 1 }
    } },

    # Trier par ordre décroissant de nombre de livres
    { "$sort": { "count": -1 } },

    # Limiter aux 10 premiers résultats
    { "$limit": 10 }
]

# Appliquer la pipeline sur la collection
result = db.books.aggregate(pipeline)

# Afficher les résultats
for doc in result:
    pprint(doc)
    

    
    