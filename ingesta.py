import os
import sys
import boto3
import pandas as pd
import mysql.connector
from mysql.connector import Error

def getenv(name, default=None):
    v = os.getenv(name, default)
    if v is None:
        return default
    return v

def main():
    host = getenv("MYSQL_HOST", "localhost")
    port = int(getenv("MYSQL_PORT", "3306"))
    user = getenv("MYSQL_USER", "root")
    password = getenv("MYSQL_PASS", "")
    database = getenv("MYSQL_DB", "")
    table = getenv("MYSQL_TABLE", "")
    output_file = getenv("OUTPUT_FILE", "data.csv")
    bucket = getenv("S3_BUCKET", "")
    s3_key = getenv("S3_KEY", output_file)

    if not database or not table or not bucket:
        print("ERROR: faltan variables requeridas. Asegúrate de definir MYSQL_DB, MYSQL_TABLE y S3_BUCKET.", file=sys.stderr)
        return 10

    print("Conectando a MySQL:", host, "DB:", database, "TABLA:", table)
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connection_timeout=10
        )
        cursor = conn.cursor()
        query = f"SELECT * FROM `{table}`;"
        cursor.execute(query)
        cols = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        df.to_csv(output_file, index=False)
        print(f"Guardado CSV local: {output_file} (filas: {len(df)})")
    except Error as e:
        print("ERROR: fallo al conectar/consultar MySQL ->", e, file=sys.stderr)
        return 20
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

    print("Subiendo a S3 bucket:", bucket, "key:", s3_key)
    try:
        s3 = boto3.client("s3")
        s3.upload_file(output_file, bucket, s3_key)
        print(f"Archivo subido a s3://{bucket}/{s3_key}")
    except Exception as e:
        print("ERROR: fallo al subir a S3 ->", e, file=sys.stderr)
        return 30

    print("Ingesta completada correctamente.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
