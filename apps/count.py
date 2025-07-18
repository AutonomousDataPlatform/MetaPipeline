import pymysql
import pymysql.cursors
import dbinfo

def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        connection = pymysql.connect(
            host=dbinfo.db_host,
            port=23306,
            user=dbinfo.db_username,
            passwd=dbinfo.db_password,
            db=dbinfo.db_name
        )

        cursor = connection.cursor()
        query = "SELECT COUNT(*) FROM frame_annotation_temp"
        cursor.execute(query)
        result = cursor.fetchone()

        total_batch = (result[0] + 49999) // 50000

        output = {'batch_number': [i for i in range(total_batch)]}
        return output

    except Exception as e:
        print("[ERROR] lambda_handler failed:", e)
        raise e

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                print("[WARNING] cursor close failed:", e)
        if connection:
            try:
                connection.close()
            except Exception as e:
                print("[WARNING] connection close failed:", e)