'''
This file contains the configuration variables.

The value of the following variables has been hidden for security reasons:
- SSH_HOST
- PRIVATE_KEY_PATH
- REMOTE_HOST
- REMOTE_PASSWORD
- LOCAL_PASSWORD
'''

# Configuring the credentials for the remote SSH server
SSH_USER = 'ubuntu'
SSH_HOST = 'xxx.xxx.xxx.xxx'
SSH_PORT = 22
PRIVATE_KEY_PATH = '/path/to/private_key.pem'

# Configuring the credentials for the remote mysql database
REMOTE_USER = 'admin'
REMOTE_HOST = 'maindbname.id.region.rds.amazonaws.com'
REMOTE_PORT = 3306
REMOTE_PASSWORD = 'remote_db_user_password'
DB = 'etl'

# Defining if to test in a local MySQL database
LOCAL_TESTING = False

# Defining the additional variables used in daily_data module
DIRECTORY_TO_WATCH = "test_dest/"
WATCHDOG_DELAY = 1  # in seconds

# Defining the additional variables used in aggregate_data module
TESTING = True  # boolean
TIME = "00:00"  # time of the day when to update the aggregate table

# Configuring the credentials for the local mysql database for local testing
LOCAL_USER = 'admin'
LOCAL_HOST = 'localhost'
LOCAL_PASSWORD = 'local_db_admin_password'
LOCAL_DB = 'test'
