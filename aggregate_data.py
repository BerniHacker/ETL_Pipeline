'''
The code calculates aggregate data (average price of the last 30 days)
by reading from the Daily_Dump table of a MySQL database daily and sends
the updated information into a dedicated table in the same database.
'''

# Importing the needed packages
import datetime as dt
import schedule
from sshtunnel import SSHTunnelForwarder
import pymysql
from pymysql.constants import CLIENT
import config  # module containing the configuration variables

# AGGREGATING


def aggregating():
    # This function creates a new aggregate table if it does not exist.
    # Then it performs a query to the database to verify if there are at least
    # 300 entries and then it calculates the average price of the last
    # 30 days. The result is loaded into the aggregate table.

    # Establishing the SSH tunnel
    # Local bind port is allocated randomly and
    # local bind host is allocated automatically
    with SSHTunnelForwarder(ssh_address_or_host=(config.SSH_HOST,
                                                 config.SSH_PORT),
                            ssh_username=config.SSH_USER,
                            ssh_private_key=config.PRIVATE_KEY_PATH,
                            remote_bind_address=(config.REMOTE_HOST,
                                                 config.REMOTE_PORT)) as tunnel:
        # Connecting to the database
        # (mysql.connector does not work through SSH tunnel)
        # (pymysql needs an explicit parameter to enable multiple queries)
        if config.LOCAL_TESTING is False:
            con = pymysql.connect(user=config.REMOTE_USER,
                                  password=config.REMOTE_PASSWORD,
                                  host=tunnel.local_bind_host,
                                  port=tunnel.local_bind_port,
                                  database=config.DB,
                                  client_flag=CLIENT.MULTI_STATEMENTS)
        else:
            # This is used for local testing
            con = pymysql.connect(user=config.LOCAL_USER,
                                  password=config.LOCAL_PASSWORD,
                                  host=config.LOCAL_HOST,
                                  database=config.LOCAL_DB,
                                  client_flag=CLIENT.MULTI_STATEMENTS)
        cur = con.cursor()

        # Executing a query to create the needed table (if not existing)
        sql1 = "CREATE TABLE IF NOT EXISTS Current_Mean_Price "\
               "(id BIGINT(20) NOT NULL PRIMARY KEY AUTO_INCREMENT, "\
               "temperature_group ENUM('LOW', 'MEDIUM', 'HIGH'), "\
               "mean_price DOUBLE, "\
               "INDEX temp_group (temperature_group)) "\
               "DEFAULT CHARACTER SET=utf8;"
        cur.execute(sql1)
        con.commit()

        # Selecting the temperature and price values of the last 300 entries
        # (last 30 days) and storing them into a temporary table
        sql2 = "CREATE TEMPORARY TABLE Last_30_Days AS "\
               "(SELECT temperature, price "\
               "FROM Daily_Dump "\
               "ORDER BY id DESC "\
               "LIMIT 300)"
        # Clearing the aggregate data table
        sql3 = "DELETE FROM Current_Mean_Price"
        # Grouping by tempetature group and calculating the mean price
        # Inserting the aggregate data into the aggregate table
        sql4 = "INSERT INTO Current_Mean_Price "\
               "(temperature_group, mean_price)"\
               "(SELECT temperature AS temperature_group, "\
               "AVG(price) AS mean_price "\
               "FROM Last_30_Days "\
               "GROUP BY temperature)"
        cur.execute(sql2)
        con.commit()
        cur.execute(sql3)
        con.commit()
        cur.execute(sql4)
        con.commit()

        # Providing consolle feedback
        print("The aggregate table has been updated at %s" % dt.datetime.now())

        # Closing the connection to the database
        con.close()


# Clearing any previously scheduled job
schedule.clear()

# Setting daily schedule
schedule.every().day.at(config.TIME).do(aggregating)
if config.TESTING:
    schedule.every().minute.at(":00").do(aggregating)
print("The schedules for updating the aggregate table have been set \n"
      "Press CTLR + C to interrupt")

try:
    while True:
        # Running all the schedules jobs
        schedule.run_pending()
except:
    # Handling program interruptions
    print("\nScheduling has been interrupted")
