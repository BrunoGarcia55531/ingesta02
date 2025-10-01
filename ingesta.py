import os
import pandas as pd

def read_from_mysql():
    import mysql.connector
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
    )
    table = os.getenv("MYSQL_TABLE")
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def main():
    output_file = os.getenv("OUTPUT_FILE", "output.csv")
    bucket = os.getenv("S3_BUCKET")

    if os.getenv("MYSQL_USE_FILE") == "1":
        print("[INFO] Usando data.csv como fuente (modo prueba)")
        df = pd.read_csv("data.csv")
    else:
        print("[INFO] Conectando a MySQL...")
        df = read_from_mysql()

    df.to_csv(output_file, index=False)
    print(f"[INFO] Datos guardados en {output_file}")

    if bucket:
        import boto3
        s3 = boto3.client("s3")
        key = os.path.basename(output_file)
        s3.upload_file(output_file, bucket, key)
        print(f"[INFO] Archivo subido a s3://{bucket}/{key}")
    else:
        print("[WARN] No se definió S3_BUCKET, se omitió la subida a S3.")

if __name__ == "__main__":
    main()
