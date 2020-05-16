import mysql.connector
import pandas as pd
import os

dirname = os.path.dirname(__file__)

miConexion = mysql.connector.connect(
    host='localhost', user='root', passwd='estudiante', db='train_data')
cur = miConexion.cursor()

try:
    filename = os.path.join(dirname, 'data/train_data.csv')
    data = pd.read_csv(filename)
    columnas = []
    table_name = "survivors"
    tabla_creada = False
    for d in data:
        if d != "Unnamed: 0":
            columnas.append(str(d))
    try:
        createsqltable = """CREATE TABLE IF NOT EXISTS """ + table_name + \
            " (" + " VARCHAR(250),".join(columnas) + " VARCHAR(250))"
        cur.execute(createsqltable)
        miConexion.commit()
        tabla_creada = True
    except mysql.connector.Error as e:
        print(e)

    if(tabla_creada):
        print("Creando registros...")

        sql = "INSERT INTO " + table_name + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for index, row in pd.read_csv(filename, usecols=columnas).iterrows():
            try:
                vals = (
                    row["PassengerId"], "Sobreviviente" if row["Survived"] == 1 else "No Sobreviviente", "M" if row["Sex"] == 1 else "F", row["Age"], row["Fare"], row["Pclass_1"], row[
                        "Pclass_2"], row["Pclass_3"], row["Family_size"], row["Title_1"], row["Title_2"], row["Title_3"], row["Title_4"], row["Emb_1"], row["Emb_2"], row["Emb_3"]
                )
                cur.execute(sql, vals)
                miConexion.commit()
            except mysql.connector.Error as e:
                print("Ha ocurrido un error al insertar en el index: {}".format(index))
        print("Registros insertados correctamente")
except OSError as err:
    print("OS error: {0}".format(err))
