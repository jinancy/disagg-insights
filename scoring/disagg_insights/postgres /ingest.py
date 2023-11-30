import requests
import psycopg2

with requests.get("http://127.0.0.1:5000/very_large_request/1000", stream=True) as r:
    # print(r.text)

    conn = psycopg2.connect(dbname="measurement_source", user="postgres", host="localhost", password="password", port="5433")

    cur = conn.cursor()

    sql = "INSERT INTO measurement_data (data_id, local_15min, grid) VALUES (%s, %s, %s)"

    buffer = ""
    for chunk in r.iter_content(chunk_size=1):
        if chunk.endswith(b'\n'):
            t = eval(buffer)
            print(t)
            # id = id+1
            cur.execute(sql, (t[0], t[1], t[2]))
            conn.commit()
            buffer = ""
        else:
            buffer += chunk.decode()

    conn.close()