"""
Importación de twits en una base de datos Mongo.

Prerequisitos:
    1. Tener instalado Python 3.7 o superior
    2. Haber creado un archivo CSV que contenga las cuentas de twitter que se desean descargar.
    3. Tener instancia en la nube de MongoDB Atlas, MongoDB instalado en local o en un servidor propio.

Author: Rocio Radulescu
"""
import time
import json
import pandas as pd
import pymongo
from twython import Twython  # Necesario instalarlo la primera vez de forma aislada: pip install Twython
import timeit

# --  Importación de Twitter app key and access token (prueba con Twitter Developer)
APP_KEY = 'xxx'
APP_SECRET = 'xxx'
OAUTH_TOKEN = 'xxx'
OAUTH_TOKEN_SECRET = 'xxx'
twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)


# -- Definición para leer datos de Twitter -----------------------------

def get_data_user_timeline_all_pages(kid, page):
    try:
        '''
        'count' especifica el número de tweets que se deben intentar recuperar, hasta un máximo de 200
 por solicitud. El valor del conteo se considera mejor como un límite para
 el número de tweets que se devolverán porque se eliminó el contenido suspendido o eliminado
 después de que el recuento ha sido aplicado Incluimos retweets en el conteo, incluso si
 include_rts no se suministra. 
        '''
        d = twitter.get_user_timeline(screen_name=kid, count="200", page=page, include_entities="true", include_rts="1")
    except Exception as e:
        print("Error reading id %s, exception: %s" % (kid, e))
        return None
    return d


# -- Configurar la base de datos Mongo --------------------------------
dbStringConnection = "mongodb+srv://xx:xx@cluster0.uswkx.mongodb.net"
dbName = 'xx'
dbCollectionA = 'twitter_Actividad_Cuentas'
dbCollectionT = 'tweets_Actividad'

client = pymongo.MongoClient(dbStringConnection)
db = client[dbName]
accounts = db[dbCollectionA]
db[dbCollectionA].create_index([('Twitter_handle', pymongo.ASCENDING)], unique=True)
tweets = db[dbCollectionT]
# índice de unicidad en la colección tweets para evitar duplicados
db[dbCollectionT].create_index([('id_str', pymongo.ASCENDING)], unique=True)

# --  Leer las cuentas de Twitter (y añadir a MongoDB si es posible) ----

df = pd.read_csv('example.csv', encoding='latin-1')

print("Intentando insertar ", len(df), " cuentas de Twitter")

repetidas = 0
cuentasTwitter = json.loads(df.T.to_json()).values()
for cuenta in cuentasTwitter:
    try:
        accounts.insert_one(cuenta)
    except pymongo.errors.DuplicateKeyError as e:
        print(e, '\n')
        repetidas += 1
print("Insertadas ", len(df) - repetidas, " cuentas de Twitter. ", repetidas, " cuentas repetidas.")

# Crea la lista de cuentas de twitter para descargar tweets
twitter_accounts = accounts.distinct('Twitter_handle')

# -- Bucle principal: Cuentas de Twitter se añaden a la colección de cuentas en MongoDB -----------------

start_time = timeit.default_timer()
starting_count = tweets.count_documents({})

for s in twitter_accounts[:len(twitter_accounts)]:
    # Fija el contador de duplicados para esta cuenta de twitter a cero
    duplicates = 0

    # Comprueba el ratio límite de llamadas por minuto en el API de twitter (900 peticiones/15-minutos)
    rate_limit = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline'][
        'remaining']
    print('\n', rate_limit, '# llamadas a la API restantes')
    print('\nLeyendo tweets enviados por: ', s, '-- index: ', twitter_accounts.index(s))
    page = 1

    # Se pueden descargar 200 tweets por llamada y hasta 3.200 tweets totales, es decir, 16 páginas por cuenta
    while page < 17:
        print("--- STARTING PAGE", page, '...llamadas a la API restantes estimadas: ', rate_limit)
        d = get_data_user_timeline_all_pages(s, page)

        if not d:
            print("No hubo tweets devueltos. Desplazandose al siguiente ID")
            break
        if len(d) == 0:  # Este registro es diferentes de las menciones y los ficheros DMS
            print("No hubo tweets devueltos. Desplazandose al siguiente ID")
            break

        # Decrementamos en 1 el contador rate_limit
        rate_limit -= 1

        # Escribimos los datos en MongoDB -- iteramos sobre cada tweet
        for entry in d:
            # Convertimos los datos de twitter a json para insertarlos en Mongo
            t = json.dumps(entry)
            loaded_entry = json.loads(t)
            # Insertamos el tweet en la base de datos -- A menos que este ya existe
            try:
                tweets.insert_one(loaded_entry)
            except pymongo.errors.DuplicateKeyError as e:
                print(e, '\n')
                duplicates += 1
                if duplicates > 9:
                    break
                pass

        print('... FINALIZADA ', page, ' PARA ORGANIZAR ', s, "-", len(d), " TWEETS")

        # Si hay muchos duplicados vamos a la siguietne cuenta
        if duplicates > 9:
            print('\n...Hay %s' % duplicates,
                  'duplicados....saltando al siguiente ID ...\n')
            # continue
            break

        page += 1
        if page > 16:
            print("NO SE ENCUENTRA AL FINAL DE LA PÁGINA 16")
            break

        # MÉTODO CRUDO PARA CONTROLAR LÍMITE DE TASA API
        # EL LÍMITE DE VELOCIDAD PARA COMPROBAR CUÁNTAS SON LAS LLAMADAS DE API SON 180, LO QUE SIGNIFICA QUE NO PODEMOS
        if rate_limit < 5:
            print('Se estiman menos de 5 llamadas API ... verifique y haga una pausa de 5 minutos si es necesario')
            rate_limit_check = \
            twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
            print('.......y el límite de ratio de acceso al API actual es: ', rate_limit_check)
            if rate_limit_check < 5:
                print('Quedan menos de 5 llamadas API ... pausando por 5 minutos')
                time.sleep(300)  # PAUSA DE 300 SEGUNDOS
                rate_limit = \
                twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline'][
                    'remaining']
                print('...Este es el límite de ratio de acceso al API después de una pausa de 5 minutos: ',
                      rate_limit)

    if rate_limit < 5:
        print('Quedan menos de 5 llamadas API estimadas ... pausando por 5 minutos...')
        time.sleep(300)  # PAUSA POR 300 SEGUNDOS

elapsed = timeit.default_timer() - start_time
print('# de minutos: ', elapsed / 60)
print("Número de nuevos tweets añadidos en esta ejecución: ", tweets.count_documents({}) - starting_count)
print("Número de tweets actuales en la BD: ", tweets.count_documents({}), '\n', '\n')

# -- Impresión del número de tweets en la base de datos por cuenta. ------

for org in db.tweets.aggregate([
    {"$group": {"_id": "$screen_name", "sum": {"$sum": 1}}}
]):
    print(org['_id'], org['sum'])