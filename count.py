import pymysql
import pymysql.cursors
import dbinfo

connection = pymysql.connect(host = dbinfo.db_host, port = 23306, user = dbinfo.db_username, passwd = dbinfo.db_password, db = dbinfo.db_name)

def lambda_handler(event, context):

    cursor = connection.cursor()

    query = "SELECT COUNT(*) FROM frame_annotation_temp"
    cursor.execute(query)

    result = cursor.fetchone()

    total_batch = (result[0] + 199999) // 200000

    output = {'batch_number':[]}
    
    for i in range(total_batch):
        output['batch_number'].append(i)

    cursor.close()
    connection.close()

    return output