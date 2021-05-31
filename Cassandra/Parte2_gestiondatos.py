"""
Creación y consulta de una base de datos Cassadra
Prerequisitos:
    1. Tener instalado cassandra y corriendo
    2. Tener instalado devcenter, con conexion y keyspace
    3. Levantar tablas de cql en devspace
    4. Instalar driver-cassandra python

Author: Rocio Radulescu
"""

from cassandra.cluster import Cluster, ResultSet
from datetime import date
#--------------------------------------------------------------
# 1)  Clases constructores de las entidades y relaciones del modelo
#--------------------------------------------------------------

# -- Clase estacion -------------------------------------------
class Estacion:
    def __init__(self, CodEstacion, Nombre):
        self.CodEstacion = CodEstacion
        self.Nombre = Nombre

# -- Clase Productor -------------------------------------------
class Productor:
    def __init__(self, CodProductor, Nombre, MediaProduccion, MaximaProduccion, Pais, OrigenEnergia ):
        self.CodProductor = CodProductor
        self.Nombre = Nombre
        self.MediaProduccion = MediaProduccion
        self.MaximaProduccion = MaximaProduccion
        self.Pais = Pais
        self.OrigenEnergia = OrigenEnergia

# -- Clase DistribucionDeRed (con foranea de Estacion)----------
class DistribucionDeRed:
    def __init__(self, CodDis, LongitudMaxima, CodEst ):
        self.CodDis = CodDis
        self.LongitudMaxima = LongitudMaxima
        self.CodEst = CodEst

# -- Clase Linea (con foranea de Distribucion)------------------
class Linea:
    def __init__(self, CodLin, Longitud, CodDis ):
        self.CodLin = CodLin
        self.Longitud = Longitud
        self.CodDis = CodDis

# -- Clase Subestacion (con foranea de Linea)--------------------
class Subestacion:
    def __init__(self, CodSub, Capacidad, CodLin ):
        self.CodSub = CodSub
        self.Capacidad = Capacidad
        self.CodLin = CodLin

# -- Clase Zona (con foranea de Provincia)-----------------------
class Zona:
    def __init__(self, ZonCod, Nombre, Municipios, ProCod ):
        self.ZonCod = ZonCod
        self.Nombre = Nombre
        self.Municipios = Municipios
        self.ProCod = ProCod

# -- Clase Provincia --------------------------------------------
class Provincia:
    def __init__(self, ProCod, Nombre, JefesProvinciales ):
        self.ProCod = ProCod
        self.Nombre = Nombre
        self.JefesProvinciales = JefesProvinciales

# -- Clase Relacion Provee - EstacionProductor ------------------
class EstacionProductor:
    def __init__(self, CodEst, CodPro ):
        self.CodEst = CodEst
        self.CodPro = CodPro

# -- Clase Relacion Distribuye - ZonaSubestacion ----------------
class ZonaSubestacion:
    def __init__(self, CodSub, ZonCod, Cantidad, Fecha ):
        self.CodSub = CodSub
        self.ZonCod = ZonCod
        self.Cantidad = Cantidad
        self.Fecha = Fecha


#--------------------------------------------------------------
# 2)  Creación de métodos de inserción de datos
#--------------------------------------------------------------

# -- Funcion de inserción entidad Productor ---------------------------
def insertProductor ():
    # -- Solicito ingreso de datos
    codProductor = int(input("Ingrese el código del productor: "))
    nombre = input("Ingrese el nombre del productor: ")
    mediaProduccion = float(input("Ingrese la media de producción: "))
    maximaProduccion = float(input("Ingrese la máxima de la producción: "))
    pais = input("Ingrese el país del productor: ")
    origenEnergia = input("Ingrese el origen de la energía: ")
    # -- Creo el objeto de la clase
    productor = Productor(codProductor, nombre, mediaProduccion, maximaProduccion, pais, origenEnergia)
    insertStatement = session.prepare("INSERT INTO rocioradulescu.productor (codpro, origen_energia, nombre, "
                                      "pais, maximoproduccion, mediaproduccion) VALUES (?, ?, ?, ?, ?, ?)")
    session.execute(insertStatement, [productor.CodProductor, productor.OrigenEnergia, productor.Nombre, productor.Pais,
                                      productor.MaximaProduccion, productor.MediaProduccion])
    insertStatement = session.prepare(
        "INSERT INTO rocioradulescu.productor_origen_energia (productor_origen_energia, productor_codpro,"
        " productor_nombre, productor_mediaproduccion) VALUES (?, ?, ?, ?)")
    session.execute(insertStatement,
                    [productor.OrigenEnergia, productor.CodProductor, productor.Nombre,
                     productor.MediaProduccion])

    # -- Para completar tablas relacionadas de Act1:
    # -- productores_estacion_distribucion
    input("--- Ahora completaremos las tablas productores_estacion_distribucion (enter) ---")
    distribucionRedLongitudmaxima = float(input("Ingrese la longitud máxima de la distribución: "))
    estacionCodEst = int(input("Ingrese el código de la estacion: "))
    distribucionReCodDistr = int(input("Ingrese el código de la distribución: "))

    insertStatement = session.prepare(
        "INSERT INTO rocioradulescu.productores_estacion_distribucion (distribuciondered_longitudmaxima, "
        "productor_codpro, estacion_codest, distribuciondered_coddis, productor_pais, productor_nombre) VALUES (?, ?, ?, ?, ?, ?)")
    exec = session.execute(insertStatement,
                    [distribucionRedLongitudmaxima, productor.CodProductor, estacionCodEst, distribucionReCodDistr,
                     productor.Pais, productor.Nombre])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)


# -- Funcion de inserción entidad Provincia ---------------------------
def insertProvincia():
    # -- Solicito ingreso de datos
    proCod = int(input("Ingrese el código de la provincia: "))
    nombre = input("Ingrese el nombre de la provincia: ")
    jefes_provinciales = set()  # coleccion de jefes_provinciales
    jefe_provincial = input("Ingrese el nombre de un jefe provincial o vacío para detenerse: ")
    while(jefe_provincial != ""):

        # -- Ahora que tengo UN jefe provincial y su provincia, agrego en provincia_jefes_provinciales
        insertStatement = session.prepare(
            "INSERT INTO rocioradulescu.provincia_jefes_provinciales (provincia_jefe_provincial, "
            "provincia_provcod, provincia_nombre) VALUES (?, ?, ?)")
        session.execute(insertStatement, [jefe_provincial, proCod, nombre])

        # -- Continuo gestionando la provincia
        jefes_provinciales.add(jefe_provincial)
        jefe_provincial = input("Introduzca otro nombre de jefe provincial o vacío para parar")

    # -- Creo el objeto de la clase
    provincia = Provincia(proCod, nombre, jefes_provinciales)
    insert_statement = session.prepare("INSERT INTO rocioradulescu.provincia (codpro, nombre, jefes_provinciales) VALUES (?, ?, ?)")
    session.execute(insert_statement, [provincia.ProCod, provincia.Nombre, provincia.JefesProvinciales])

    # tabla: jefes_provinciales_provincia
    insertStatement = session.prepare(
        "INSERT INTO rocioradulescu.jefes_provinciales_provincia (provincia_nombre, provincia_codpro, "
        "provincia_jefes_provinciales) VALUES (?, ?, ?)")
    session.execute(insertStatement, [provincia.Nombre, provincia.ProCod, provincia.JefesProvinciales])

    # -- Para completar tablas relacionadas de Act1:
    # jefes_provinciales_zona, provincias_linea
    input("---- Ahora completaremos las tablas jefes_provinciales_zona, provincias_linea (enter) ----")
    zonaNombre = input("Ingrese el nombre de la zona de los jefes provinciales: ")
    zonaZonCod = int(input("Ingrese el codigo de la zona de los jefes provinciales: "))
    lineaLong = float(input("Ingrese la longitud de la linea: "))
    subestacionCod = int(input("Ingrese el código de la subestacion: "))

    insertStatement = session.prepare(
        "INSERT INTO rocioradulescu.jefes_provinciales_zona (zona_nombre, zona_zoncod, provincia_jefes_provinciales) VALUES (?, ?, ?)")
    session.execute(insertStatement, [zonaNombre, zonaZonCod, provincia.JefesProvinciales])
    insertStatement = session.prepare(
        "INSERT INTO rocioradulescu.provincias_linea (linea_longitud, subestacion_codsub, zona_zoncod,"
        "provincia_nombre ,provincia_procod) VALUES (?, ?, ?, ?, ?)")
    exec = session.execute(insertStatement, [lineaLong, subestacionCod, zonaZonCod, provincia.Nombre, provincia.ProCod ])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)



def insertDivide():
    # -- La relacion divide es un 1:n de Provincia - Zona. La clave las tengo en Zona
    # -- Sin tablas soporte, solo completo la tabla provincias_linea
    lineaLong = float(input("Ingrese la longitud de la linea: "))
    subestacionCoSubs = int(input("Ingrese el codigo de la subestacion: "))
    proCod = int(input("Ingrese el código de la provincia: "))
    proNombre = input("Ingrese el nombre de la provincia: ")
    zonaZonCod = int(input("Ingrese el codigo de la provincia: "))

    insert_statement = session.prepare("INSERT INTO rocioradulescu.provincias_linea (linea_longitud,"
                                       "subestacion_codsub, zona_zoncod, provincia_nombre,"
                                       " provincia_procod) VALUES (?, ?, ?, ?, ?)")
    exec = session.execute(insert_statement, [lineaLong, subestacionCoSubs,zonaZonCod,proNombre, proCod])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)


def insertCabecera():
    # -- La relacion cabecera es un 1:n de Estacion - Distribucion.
    # -- Sin tablas soporte, solo completo la tabla productores_estacion_distribucion
    distLongitudMaxima = float(input("Ingrese la longitud maxima de la distribución: "))
    productorCodpro = int(input("Ingrese el codigo del productor: "))
    estacionCodest = int(input("Ingrese el codigo de la estacion: "))
    distCoddis = int(input("Ingrese el codigo de la distribucion de red: "))
    productorPais = input("Ingrese el pais del productor: ")
    productorNombre = input("Ingrese el nombre del productor: ")

    insert_statement = session.prepare("INSERT INTO rocioradulescu.productores_estacion_distribucion("
                                       " distribuciondered_longitudmaxima,productor_codpro, estacion_codest,"
                                       "distribuciondered_coddis, productor_pais, "
                                       "productor_nombre) VALUES (?, ?, ?, ?, ?, ?)")
    exec = session.execute(insert_statement, [distLongitudMaxima, productorCodpro, estacionCodest, distCoddis,
                                       productorPais, productorNombre])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)

def insertSuple():
    # -- La relacion cabecera es un 1:n de Linea - Subestacion.
    # -- Utilizo un ejemplo con tablas soporte, suple_subestacion
    codSub = int(input("Ingrese el codigo de la subestacion: "))
    codLin = int(input("Ingrese el codigo de la linea: "))
    capacidad = float(input("Ingrese la capacidad de la subestacion: "))
    insert_statement = session.prepare("INSERT INTO rocioradulescu.suple_subestacion("
                                       " codsub, codlin, capacidad) VALUES (?, ?, ?)")
    exec = session.execute(insert_statement, [codSub,codLin,capacidad])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)


def insertDistribuye():
    # -- La relacion distribuye es un n:m de Subestacion - Zona.
    # -- Utilizo tablas soporte por relacion n:m
    fecha = input("Ingrese la fecha de la operacion (formato yyyy-mm-dd): ")
    codSub = int(input("Ingrese el codigo de la subestacion: "))
    zonCod = int(input("Ingrese el codigo de la zona: "))
    cantidad = float(input("Ingrese la cantidad de la operacion: "))
    distribuye = ZonaSubestacion (codSub, zonCod,cantidad,fecha)
    insert_statement = session.prepare("INSERT INTO rocioradulescu.distribuye (codsub, zoncod,"
                                       " cantidad, fecha) VALUES (?, ?, ?, ?)")
    exec = session.execute(insert_statement, [distribuye.CodSub, distribuye.ZonCod, distribuye.Cantidad, distribuye.Fecha])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)


def insertCabeceraConsisteSuple():
    # -- Esta relacion se da entre Estacion - Distribucion - Linea - Subestacion, por 1:n.
    # -- Sin tablas soporte, solo utilizo la tabla subestaciones_estacion_linea
    estNombre = input("Ingrese el nombre de la estacion: ")
    subCod = int(input("Ingrese el codigo de la subestación: "))
    estacCod = int(input("Ingrese el codigo de la estacion: "))
    lineaLon = float(input("Ingrese la longitud de la linea: "))
    insert_statement = session.prepare("INSERT INTO rocioradulescu.subestaciones_estacion_linea ("
                                       "estacion_nombre, subestacion_codsub, estacion_codest, linea_longitud) VALUES (?, ?, ?, ?)")
    exec = session.execute(insert_statement, [estNombre,subCod,estacCod,lineaLon])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)


def insertSupleDistribuyeDivide():
    # -- Esta relacion se da entre   Linea - Subestacion - Zona - Provincia.
    # -- Sin tablas soporte, solo utilizo la tabla provincias_linea
    lineaLongitud = float(input("Ingrese la longitud de la linea: "))
    subCod = int(input("Ingrese el código de la subestación: "))
    zonaCod = int(input("Ingrese el código de la zona: "))
    provNombre = input("Ingrese el nombre de la provincia: ")
    provCod = int(input("Ingrese el código de la provincia: "))
    insert_statement = session.prepare("INSERT INTO rocioradulescu.provincias_linea (linea_longitud,"
                                       "subestacion_codsub, zona_zoncod, provincia_nombre, "
                                       "provincia_procod) VALUES (?, ?, ?, ?, ?)")
    exec = session.execute(insert_statement, [lineaLongitud,subCod, zonaCod,provNombre,provCod])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)


def insertProveeCabecera():
    # -- Esta relacion se da entre Productor - Estacion - Distribucion.
    # -- Sin tablas soporte, solo utilizo la tabla productores_estacion_distribucion
    disLongMAx = float(input("Ingrese la longitud máxima de la distribución: "))
    prodCod = int(input("Ingrese el código del productor: "))
    estCod = int(input("Ingrese el código de la estación: "))
    disCod = int(input("Ingrese el código de la distribución: "))
    prodPais = input("Ingrese el pais del productor: ")
    prodNombre = input("Ingrese el nombre del productor: ")
    insert_statement = session.prepare("INSERT INTO rocioradulescu.productores_estacion_distribucion ("
                                       "distribuciondered_longitudmaxima, productor_codpro, estacion_codest,"
                                       "distribuciondered_coddis, productor_pais,productor_nombre)"
                                       " VALUES (?, ?, ?, ?, ?, ?)")
    exec = session.execute(insert_statement, [disLongMAx, prodCod,estCod,disCod,prodPais,prodNombre])
    if exec != 0:
        print("Los datos se ingresaron en la base correctamente.")
    else:
        print("Se produjo un error: ", exec)

#--------------------------------------------------------------
# 3) Creación de métodos destinados a consultar la información de tablas soporte
#   Provincia - Productores - Distribuye
#--------------------------------------------------------------


def consultarDatosProvincia(cod_provincia):
    select = session.prepare("SELECT codpro, nombre, jefes_provinciales FROM provincia WHERE codpro = ?")
    filas = session.execute(select, [cod_provincia,])
    for fila in filas:
        provincia = Provincia(cod_provincia, fila.nombre, fila.jefes_provinciales)
        return provincia


def consultarDatosProductor(cod_productor):
    select = session.prepare("SELECT codpro, origen_energia, nombre, pais, maximoproduccion,"
                             "mediaproduccion FROM productor WHERE codpro = ?")
    filas = session.execute(select, [cod_productor,])
    for fila in filas:
        productor = Productor(cod_productor, fila.origen_energia, fila.nombre, fila.pais,
                              fila.maximoproduccion, fila.mediaproduccion)
        return productor

def consultarDatosDistribuye(cod_subest, cod_zona):
    select = session.prepare("SELECT codsub, zoncod, cantidad, fecha FROM distribuye WHERE codsub = ? and zoncod = ?")
    filas = session.execute(select, [cod_subest, cod_zona,])
    for fila in filas:
        dis = ZonaSubestacion(cod_subest,cod_zona, fila.cantidad, fila.fecha)
        return dis

#--------------------------------------------------------------
# 4) Creación de funciones de actualización de datos:
# - Actualizar el nombre de una provincia.
# - Actualizar la capacidad de una subestación.
# - Actualizar el origen de energía de un productor
#--------------------------------------------------------------

def actualizarNombreProvincia():
    nombre = input("Ingrese el nuevo nombre de la provincia: ")
    codProv = int(input("Ingrese el código de la provincia a editar: "))
    provincia = consultarDatosProvincia(codProv)
    if (provincia != None): # Si ingreso un codigo correcto
        # --Tabla soporte
        updateNombreProvincia = session.prepare("UPDATE provincia SET nombre = ? WHERE codpro = ?")
        session.execute(updateNombreProvincia, [nombre, provincia.ProCod])

        # Tabla jefes_provinciales_provincia
        borrarProvincia = session.prepare("DELETE FROM jefes_provinciales_provincia WHERE nombre = ? AND codpro = ?")
        session.execute(borrarProvincia, [provincia.Nombre,codProv])
        insertStatement = session.prepare("INSERT INTO jefes_provinciales_provincia (codpro, nombre, jefes_provinciales) VALUES (?, ?, ?)")
        session.execute(insertStatement, [codProv, provincia.Nombre, provincia.JefesProvinciales])


# -- Funcion para obtener las subestaciones por codigo
def consultarDatosSubestacion(codSub):
    select = session.prepare("SELECT * FROM suple_subestacion WHERE codsub = ?")
    fila = session.execute(select, [codSub,])
    subestacion = Subestacion(codSub, fila.capacidad, fila.codlin)
    return subestacion


def consultarSubestacionLinea(Capacidad, codSub):
    select = session.prepare("SELECT * FROM subestacion_longitud_lineas WHERE subestacion_capacidad = ? and subestacion_codsub = ?")
    fila = session.execute(select, [Capacidad, codSub,])
    lineasubestacion = fila.linea_longitud
    return lineasubestacion


def actualizarCapacidad(codSub, capacidad):
    fecha = input("Ingrese la fecha de la distribucion (yyyy-mm-dd): ")
    zonCod = int(input("Ingrese el código de la zona: "))
    select = session.prepare("SELECT * FROM zonas_subestaciones_fecha"
                             " WHERE distribuye_fecha = ? and subestacion_codsub = ? and zona_zoncod = ?")
    filas = session.execute(select, [fecha, codSub, zonCod])
    for fila in filas:
        updateValorCapacidad = session.prepare("UPDATE zonas_subestaciones_fecha SET capacidad = ?"
                                               " WHERE distribuye_fecha = ? and subestacion_codsub = ? and zona_zoncod = ?")
        session.execute(updateValorCapacidad, [capacidad, fecha, codSub, zonCod])


def actualizarSumaCapacidadZona(capacidadAnterior, capacidadNueva):
    zonCod = int(input("Ingrese el código de la zona: "))
    selectConteo = session.prepare("SELECT num_subestacion_capacidad FROM capacidad_subestaciones_zona"
                             " WHERE zona_zoncod = ?")
    fila = session.execute(selectConteo, [zonCod])
    nuevaSuma = fila.num_subestacion_capacidad - capacidadAnterior + capacidadNueva
    updateConteo = session.prepare("UPDATE capacidad_subestaciones_zona SET num_subestacion_capacidad = ?"
                                           " WHERE zona_zoncod = ?")
    session.execute(updateConteo, [nuevaSuma, zonCod])


def actualizarCapacidadSubestacion():
    capacidad = float(input("Ingrese el nuevo valor de capacidad: "))
    codSub = int(input("Ingrese el código de la subestacion a editar: "))
    subestacion = consultarDatosSubestacion(codSub)
    if (subestacion != None): # Si ingreso un codigo correcto
        # --Tabla soporte
        updateValorCapacidad = session.prepare("UPDATE suple_subestacion SET capacidad = ? WHERE codsub = ?")
        session.execute(updateValorCapacidad, [capacidad, subestacion.CodSub])

        # Tabla subestacion_longitud_lineas (al ser pk, primero borrar y despues insertar para actualizar)
        lineaSub = consultarSubestacionLinea(subestacion.Capacidad, codSub)
        borrarSubestacion = session.prepare("DELETE FROM subestacion_longitud_lineas WHERE subestacion_capacidad = ? AND subestacion_codsub = ?")
        session.execute(borrarSubestacion, [subestacion.Capacidad,codSub])
        insertStatement = session.prepare("INSERT INTO subestacion_longitud_lineas (subestacion_capacidad,"
                                          "subestacion_codsub, linea_longitud) VALUES (?, ?, ?)")
        session.execute(insertStatement, [capacidad, codSub, lineaSub])

        # Tabla zonas_subestaciones_fecha (update)
        actualizarCapacidad(codSub, capacidad)

        # Tabla capacidad_subestaciones_zona (update conteo)
        actualizarSumaCapacidadZona(subestacion.Capacidad,capacidad)


def actualizarOrigenEnergiaProductor():
    origen = input("Ingrese el nuevo origen de energia de la productor: ")
    codProd = int(input("Ingrese el código del productor a editar: "))
    productor = consultarDatosProductor(codProd)
    if (productor != None): # Si ingreso un codigo correcto
        # --Tabla soporte
        updateNombreProvincia = session.prepare("UPDATE productor SET origen_energia = ? WHERE codpro = ?")
        session.execute(updateNombreProvincia, [origen, productor.ProCod])

        # Tabla productor_origen_energia
        borrarProductor = session.prepare("DELETE FROM productor_origen_energia WHERE "
                                          "productor_origen_energia = ? AND productor_codpro = ?")
        session.execute(borrarProductor, [productor.OrigenEnergia,codProd])
        insertStatement = session.prepare("INSERT INTO productor_origen_energia (productor_origen_energia,"
                                          "productor_codpro, productor_nombre, productor_mediaproduccion)"
                                          " VALUES (?, ?, ?, ?)")
        session.execute(insertStatement, [origen, codProd, productor.Nombre, productor.MediaProduccion])

#--------------------------------------------------------------
# 5) Creación de funciones de consulta de información general
# Como ejemplo hice de las tablas soporte: provincia, productor
# y de las tablas de Actividad 1: ej1, ej3, ej6, ej9
#--------------------------------------------------------------


def consultaProvinciaPorCod():
    codPro = int(input ("Ingrese el código de la provincia a consultar: "))
    provincia = consultarDatosProvincia(codPro)
    if (provincia != None):
        print("Codigo de provincia: ", provincia.ProCod)
        print("Nombre: ", provincia.Nombre)
        print("Jefes provinciales: ", provincia.JefesProvinciales)
    else:
        print("No se ha encontrado el código ingresado.")


def consultaProductorPorCod():
    codPro = int(input("Ingrese el código del productor a consultar: "))
    productor = consultarDatosProductor(codPro)
    if (productor != None):
        print("Codigo de productor: ", productor.CodProductor)
        print("Origen de energia: ", productor.OrigenEnergia)
        print("Nombre: ", productor.OrigenEnergia)
        print("País: ", productor.Pais)
        print("Maximo de produccion:", productor.MaximaProduccion)
        print("Media de produccion:", productor.MediaProduccion)

    else:
        print("No se ha encontrado el código ingresado.")


def consultaJefesProvincialesProvincia ():
    proNom = input("Ingrese el nombre de la provincia a buscar: ")
    proCod = int(input("Ingrese el código de la provincia a buscar: "))
    select = session.prepare("SELECT * FROM jefes_provinciales_provincia"
                                   " WHERE provincia_nombre = ? and provincia_codpro=")
    fila = session.execute(select, [proNom, proCod])
    if(fila != None):
        print("Codigo de provincia: ", fila.provincia_codpro)
        print("Nombre de provincia: ", fila.provincia_nombre)
        print("Jefes provinciales:", fila.jefes_provinciales)


def consultaCapacidadSubZona():
    zonCod = int(input("Ingrese el código de zona a buscar: "))
    select = session.prepare("SELECT * FROM capacidad_subestaciones_zona"
                                   " WHERE zona_zoncod = ?")
    fila = session.execute(select, [zonCod])
    if (fila != None):
        print("Codigo de zona: ", fila.zona_zoncod)
        print("Capacidad de subestaciones asociadas: ", fila.num_subestacion_capacidad)


def consultaZonaEnergiaFecha():
    disFecha = input("Ingrese la fecha de distribución (yyyy-mm-dd):")
    subCod = int(input("Ingrese el codigo de subestación: "))
    zonCod = int(input("Ingrese el código de zona a buscar: "))
    select = session.prepare("SELECT * FROM zonas_subestaciones_fecha"
                                   " WHERE distribuye_fecha = ? and "
                                    "subestacion_codsub = ? and zona_zoncod = ?")
    fila = session.execute(select, [disFecha, subCod, zonCod])
    if (fila != None):
        print("Codigo de zona: ", fila.zona_zoncod)
        print("Código de la subestación: ", fila.subestacion_codsub)
        print("Nombre de zona: ", fila.zona_nombre)
        print("Subestacion capacidad: ", fila.subestacion_capacidad)
        print("Capacidad de subestaciones asociadas: ", fila.num_subestacion_capacidad)


def consultaCantidadDistribucionesEstacion():
    estCod = int(input("Ingrese el código de la estación a buscar: "))
    select = session.prepare("SELECT * FROM distribucion_red_estacion"
                                   " WHERE estacion_codest = ?")
    fila = session.execute(select, [estCod])
    if (fila != None):
        print("Codigo de estacion: ", fila.estacion_codest)
        print("Cantidad de distribuciones de red por subestación: ", fila.num_distribucion_de_red_coddis)

#--------------------------------------------------------------
# 6) Creación de programa principal e interfaz de usuario
#--------------------------------------------------------------

# -- Conexión con Cassandra
cluster = Cluster()
#cluster = Cluster(['192.168.0.1', '192.168.0.2'], port=..., ssl_context=...)
session = cluster.connect('')
numero = -1

print(" ------------------ BIENVENIDO ------------------ ")
while (numero != 0):
    print("""\n \n Ingrese un número para ejecutar una de las siguientes operaciones (0 para salir):
    1. Insertar un productor
    2. Insertar una provincia
    3. Insertar relación Divide 
    4. Insertar relación Cabecera
    5. Insertar relación Suple
    6. Insertar relación Distribuye 
    7. Insertar relacion ProveeCabecera entre Productor - Estacion - Distribucion
    8. Insertar relacion CabeceraConsisteSuple entre Estacion - Distribucion - Linea - Subestacion
    9. Insertar relacion SupleDistribuyeDivide entre Linea - Subestacion - Zona - Provincia
    10. Consulte datos del Productor
    11. Consulte datos de la Provincia
    12. Actualice el nombre de una provincia
    13. Actualice la capacidad de una subestación
    14. Actualice el origen de energía de un productor
    15. Consultar la suma de capacidades de subestaciones para una zona 
    16. Consultar los jejes provinciales asociados a una provincia
    17. Consultar cantidad de distribuciones de red por estación
    18. Consultar zonas y subestaciones en las que se suministraron energía en una fecha determinada    
    0. Cerrar aplicación""")

    numero = int(input()) #Pedimos valor al usuario

    if(numero == 1):
        insertProductor()
    elif(numero == 2):
        insertProvincia()
    elif(numero == 3):
        insertDivide()
    elif (numero == 4):
        insertCabecera()
    elif (numero == 5):
        insertSuple()
    elif(numero == 6):
        insertDistribuye()
    elif(numero == 7):
       insertProveeCabecera()
    elif(numero == 8):
        insertCabeceraConsisteSuple()
    elif(numero == 9):
        insertSupleDistribuyeDivide()
    elif(numero == 10):
        consultaProductorPorCod()
    elif(numero == 11):
        consultaProvinciaPorCod()
    elif(numero == 12):
        actualizarNombreProvincia()
    elif(numero == 13):
        actualizarCapacidadSubestacion()
    elif(numero == 14):
        actualizarOrigenEnergiaProductor()
    elif(numero == 15):
        consultaCapacidadSubZona()
    elif(numero == 16):
        consultaJefesProvincialesProvincia()
    elif(numero == 17):
        consultaCantidadDistribucionesEstacion()
    elif(numero == 18):
        consultaZonaEnergiaFecha()
    elif(numero == 0):
        print(" -----------Gracias por utilizar la aplicación. ----------")
        break
    else:
        print("Valor incorrecto.")

cluster.shutdown() #cerramos conexion