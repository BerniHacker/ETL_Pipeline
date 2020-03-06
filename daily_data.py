'''
This code monitors a defined subdirectory for any new CSV files, extracts
the content of such files into a pandas dataframe, extracts the date from
the file name and appends it into a separate column in the dataframe,
does some formatting to the dataframe and then
sends the data into a defined MySQL database.
'''

# Importing the needed packages
import time
import datetime as dt
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import pymysql
from pymysql.constants import CLIENT
import config  # module containing the configuration variables

# MONITORING for new files in the defined directory


class Watcher:
    # This waits for any events on the watched directory and calls Handler
    # when the event happens.

    watched_dir = config.DIRECTORY_TO_WATCH
    delay = config.WATCHDOG_DELAY

    def __init__(self):
        # Initializing the attributes of the class
        self.observer = Observer()

    def run(self):
        print("Watching the directory:", self.watched_dir, "\n"
              "Press CTLR + C to interrupt")
        # Referring to the next class
        event_handler = Handler()
        # Defining what to watch and what to do when the event happens
        self.observer.schedule(event_handler, self.watched_dir)
        # Starting the  watch
        self.observer.start()
        try:
            while True:
                # Introducing a delay before the next check
                time.sleep(self.delay)
        except:
            # Stopping the watch
            self.observer.stop()
            print("\nMonitoring has been interrupted")
        # Waiting until the thread terminates
        self.observer.join()


class Handler(FileSystemEventHandler):
    # This is the event handler that takes action when an event is received.
    # The action is to call the function fetch when a new file is found
    # in the specified directory.

    @staticmethod
    def on_any_event(event):
        # Subdirectories are not monitored
        if event.is_directory:
            return None
        # Checking for new files
        # (new directories are not monitored)
        elif event.event_type == 'created':
            # Providing consolle feedback (file name and date/time)
            print("\nNew file detected: %s" % event.src_path)
            print("at %s" % dt.datetime.now())
            # Calling the function fetch when a new file is detected
            fetch(event.src_path)


# EXTRACTING and TRANSFORMING

def fetch(self):
    # This function takes as input a file name with path and performs
    # the following tasks:
    # dumps the content of the file into a dataframe and then appends
    # this dataframe to a dataframe with a defined template,
    # extracts the date from the file name and inserts it into the dataframe,
    # does some formatting for compatibility with SQL,
    # reorders the dataframe columns,
    # calls a function to write the result into a database.

    # Defining a dataframe template
    df_templ = pd.DataFrame(columns=['date',
                                     'temperature',
                                     'at_risk',
                                     'skipped_beat',
                                     'price'])
    # Defining the data types of each column
    df_templ['date'] = df_templ['date'].astype('datetime64')
    df_templ['temperature'] = df_templ['temperature'].astype('category')
    df_templ['at_risk'] = df_templ['at_risk'].astype('bool')
    df_templ['skipped_beat'] = df_templ['skipped_beat'].astype('int64')
    df_templ['price'] = df_templ['price'].astype('float64')

    # Copying the CSV content into a dataframe
    loaded_df = pd.read_csv(self)
    # Appending the loaded dataframe to the template dafatrame
    # ('sort=True' is necessary since the columns are not always
    # in the same order)
    df_templ = df_templ.append(loaded_df, sort=True, ignore_index=True)

    # Extracting the date
    date_info = os.path.splitext(os.path.basename(self))[0]
    # Adding the date in the dataframe content
    df_templ['date'] = date_info

    # Changing 'NaN' into 'None' for compatibility with MySQL and
    # ensuring all the columns are of type object
    df_templ = df_templ.astype(object).where((pd.notnull(df_templ)), None)

    # Ordering the columns
    df_templ = df_templ.reindex(columns=['date',
                                         'temperature',
                                         'at_risk',
                                         'skipped_beat',
                                         'price'])

    # Calling the function dump to copy the data into a MySQL table
    dump(df_templ)


# LOADING

def dump(self):
    # This function takes a dataframe and creates a MySQL table
    # (if not existing) containing the needed columns with the proper format
    # and includes some index.
    # Then the function dumps the content of the input dataframe into the table.
    # The connection to the database is done through a SSH tunnel.

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
            conn = pymysql.connect(user=config.REMOTE_USER,
                                   password=config.REMOTE_PASSWORD,
                                   host=tunnel.local_bind_host,
                                   port=tunnel.local_bind_port,
                                   database=config.DB,
                                   client_flag=CLIENT.MULTI_STATEMENTS)
        else:
            # This is used for local testing
            conn = pymysql.connect(user=config.LOCAL_USER,
                                   password=config.LOCAL_PASSWORD,
                                   host=config.LOCAL_HOST,
                                   database=config.LOCAL_DB,
                                   client_flag=CLIENT.MULTI_STATEMENTS)
        cur = conn.cursor()

        # Executing a query to create the needed table (if not existing)
        # (indexes are added for the columns date, temperature and at_risk,
        # one multiple column index is added to allow smoothly extraction
        # of data for calculatimg aggregate values)
        sql1 = "CREATE TABLE IF NOT EXISTS Daily_Dump "\
               "(id BIGINT(20) NOT NULL PRIMARY KEY AUTO_INCREMENT, "\
               "date DATETIME, "\
               "temperature ENUM('LOW', 'MEDIUM', 'HIGH'), "\
               "at_risk BOOL, "\
               "skipped_beat SMALLINT, "\
               "price DOUBLE, "\
               "INDEX day (date), "\
               "INDEX temp_group (temperature), "\
               "INDEX risk (at_risk), "\
               "INDEX create_aggr_data (temperature, price)) "\
               "DEFAULT CHARACTER SET=utf8;"
        cur.execute(sql1)

        # Looping through all the indexes of the dataframe
        for ind in self.index.tolist():
            # Preparing the values for the database
            values = (self.iloc[ind, 0],  # date
                      self.iloc[ind, 1],  # temperature
                      self.iloc[ind, 2],  # at_risk
                      self.iloc[ind, 3],  # skipped_beat
                      self.iloc[ind, 4])  # price
            # Inserting the entry into the database table
            sql2 = "INSERT INTO Daily_Dump "\
                   "(date, "\
                   "temperature, "\
                   "at_risk, "\
                   "skipped_beat, "\
                   "price) "\
                   "VALUES (%s, %s, %s, %s, %s)"
            cur.execute(sql2, values)

        # Committing the queries
        conn.commit()
        print("File loaded into the database")

        # Closing the connection to the database
        conn.close()


# Running the script directly
if __name__ == '__main__':
    Watcher().run()
