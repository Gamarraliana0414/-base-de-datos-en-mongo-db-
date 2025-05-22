from pymongo import MongoClient

# Conexión al servidor MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Crear o usar una base de datos llamada "peliculas_db"
db = client["peliculas_db"]

# Crear o usar una colección llamada "peliculas"
peliculas_collection = db["peliculas"]

# Elimina la colección si ya existe, para evitar duplicados al probar varias veces
peliculas_collection.drop()

# Lista de películas (arreglo corregido con comas donde faltaban)
todas_peliculas = [
    {"titulo": "Inception", "director": "Christopher Nolan", "anio": 2010},
    {"titulo": "The Matrix", "director": "Lana Wachowski, Lilly Wachowski", "anio": 1999},
    {"titulo": "Interstellar", "director": "Christopher Nolan", "anio": 2014},
    {"titulo": "The Godfather", "director": "Francis Ford Coppola", "anio": 1972},
    {"titulo": "Pulp Fiction", "director": "Quentin Tarantino", "anio": 1994},
    {"titulo": "The Dark Knight", "director": "Christopher Nolan", "anio": 2008},
    {"titulo": "Fight Club", "director": "David Fincher", "anio": 1999},
    {"titulo": "Forrest Gump", "director": "Robert Zemeckis", "anio": 1994},
    {"titulo": "The Shawshank Redemption", "director": "Frank Darabont", "anio": 1994},
    {"titulo": "The Lord of the Rings: The Return of the King", "director": "Peter Jackson", "anio": 2003},
    {"titulo": "The Social Network", "director": "David Fincher", "anio": 2010},
    {"titulo": "The Silence of the Lambs", "director": "Jonathan Demme", "anio": 1991},
    {"titulo": "Gladiator", "director": "Ridley Scott", "anio": 2000},
    {"titulo": "The Departed", "director": "Martin Scorsese", "anio": 2006},
    {"titulo": "The Prestige", "director": "Christopher Nolan", "anio": 2006},
    {"titulo": "The Lion King", "director": "Roger Allers, Rob Minkoff", "anio": 1994},
    {"titulo": "Saving Private Ryan", "director": "Steven Spielberg", "anio": 1998},
    {"titulo": "Jurassic Park", "director": "Steven Spielberg", "anio": 1993},
    {"titulo": "Avatar", "director": "James Cameron", "anio": 2009},
    {"titulo": "Titanic", "director": "James Cameron", "anio": 1997},
    {"titulo": "Star Wars: Episode IV - A New Hope", "director": "George Lucas", "anio": 1977},
    {"titulo": "The Avengers", "director": "Joss Whedon", "anio": 2012},
    {"titulo": "Black Panther", "director": "Ryan Coogler", "anio": 2018},
    {"titulo": "Memento", "director": "Christopher Nolan", "anio": 2000},
    {"titulo": "The Usual Suspects", "director": "Bryan Singer", "anio": 1995},
    {"titulo": "Se7en", "director": "David Fincher", "anio": 1995},
    {"titulo": "The Green Mile", "director": "Frank Darabont", "anio": 1999},
    {"titulo": "The Intouchables", "director": "Olivier Nakache, Éric Toledano", "anio": 2011},
    {"titulo": "Whiplash", "director": "Damien Chazelle", "anio": 2014},
    {"titulo": "The Wolf of Wall Street", "director": "Martin Scorsese", "anio": 2013},
    {"titulo": "The Shape of Water", "director": "Guillermo del Toro", "anio": 2017},
    {"titulo": "Parasite", "director": "Bong Joon-ho", "anio": 2019}
]

# Insertar todas las películas
peliculas_collection.insert_many(todas_peliculas)

# Mostrar todas las películas
print("\n--- TODAS LAS PELÍCULAS ---")
for pelicula in peliculas_collection.find():
    print(pelicula)

# Mostrar solo películas de Ciencia Ficción (aunque ninguna tiene ese género todavía)
print("\n--- PELÍCULAS DE CIENCIA FICCIÓN ---")
for pelicula in peliculas_collection.find({"genero": "Ciencia Ficción"}):
    print(pelicula)

# Mostrar películas del año 2000 en adelante
print("\n--- PELÍCULAS DEL AÑO 2000 EN ADELANTE ---")
for pelicula in peliculas_collection.find({"anio": {"$gte": 2000}}):
    print(pelicula)

# Actualizar el director de The Matrix
peliculas_collection.update_one(
    {"titulo": "The Matrix"},
    {"$set": {"director": "Lana Wachowski y Lilly Wachowski"}}
)
print("\n✔ Director actualizado.")

# Incrementar en 1 el año de todas las películas
peliculas_collection.update_many(
    {},
    {"$inc": {"anio": 1}}
)
print("✔ Año actualizado en todas las películas.")

# Eliminar una película específica
peliculas_collection.delete_one({"titulo": "Parasite"})
print("✔ Película 'Parasite' eliminada.")

# Eliminar todas las de Ciencia Ficción (aunque no hay ninguna aún con ese género)
peliculas_collection.delete_many({"genero": "Ciencia Ficción"})
print("✔ Películas de Ciencia Ficción eliminadas.")

# Agrupar por género
print("\n--- AGRUPADAS POR GÉNERO ---")
pipeline = [
    {"$group": {"_id": "$genero", "total": {"$sum": 1}}}
]
for result in peliculas_collection.aggregate(pipeline):
    print(result)

# Agrupar por año
print("\n--- AGRUPADAS POR AÑO ---")
pipeline = [
    {"$group": {"_id": "$anio", "peliculas": {"$push": "$titulo"}}}
]
for result in peliculas_collection.aggregate(pipeline):
    print(result)

# ----------------------------
# CONSULTAS DE LA ACTIVIDAD
# ----------------------------

# 1. Contar cuántas películas hay por director
print("\n--- CANTIDAD DE PELÍCULAS POR DIRECTOR ---")
pipeline = [
    {"$group": {"_id": "$director", "total": {"$sum": 1}}}
]
for result in peliculas_collection.aggregate(pipeline):
    print(result)

# 2. Buscar películas que contengan la palabra "The"
print("\n--- PELÍCULAS QUE CONTIENEN 'The' EN EL TÍTULO ---")
for pelicula in peliculas_collection.find({"titulo": {"$regex": "The", "$options": "i"}}):
    print(pelicula)

# 3. Consultar las películas de Christopher Nolan y ordenarlas por año descendente
print("\n--- PELÍCULAS DE CHRISTOPHER NOLAN (ORDENADAS POR AÑO DESCENDENTE) ---")
for pelicula in peliculas_collection.find({"director": "Christopher Nolan"}).sort("anio", -1):
    print(pelicula)
