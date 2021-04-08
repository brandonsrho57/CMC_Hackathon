import snowflake.connector as sf
from config import config

# Connection String
conn = sf.connect(
    user=config.username,
    password=config.password,
    account=config.account
)

def test_connection(connect, query):
    cursor = connect.cursor()
    cursor.execute(query)
    cursor.close()

try:
    sql = """
    SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGSCOREVALUE"
    LIMIT 5;
    """
    test_connection(conn, sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    for line in cursor:
        print(line)
except Exception as e:
    print(e)