import logging
import sys
from datetime import date, datetime

from database.local_db_helper import LocalDBHelper
from helpers.datamart_helper import DataMartHelper
from helpers.datawarehouse_helper import DataWarehouseHelper


def log(message, level):
    if True:
        print(message)
    if level == "INFO":
        logging.info(message)
    elif level == "ERROR":
        logging.error(message)


def processing(alias, db_name, db_url, db_user, port, id_date):
    try:
        log("Start data transfer from DWH to DM [source : TWITTER, client : " + alias + ", date : " + date + "]",
            "INFO")
        datamart_helper = DataMartHelper(alias, db_name, db_url, db_user, port)
        datamart_helper.insert_fact_record_twitter(id_date)
        datamart_helper.insert_fact_frequency(id_date)
    except Exception as e:
        log("Error occurred while transferring data from DWH to DM [source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
            e), "ERROR")
        return


logfile = "log/DM_" + str(date.today()) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s (%(name)s) ->  %(levelname)s :: %(message)s')

try:
    local_db_helper = LocalDBHelper()
    dwh_helper = DataWarehouseHelper()
except Exception as e:
    log("Error occurred during Fetching Helpers initialization : " + str(e), "ERROR")
    sys.exit(-1)

date = datetime.today().strftime('%Y-%m-%d')
id_date = dwh_helper.get_id_date(date)
for client in local_db_helper.get_active_clients():
    alias = client[1]
    db_name = client[4]
    db_url = client[5]
    db_user = client[6]
    port = client[7]
    processing(alias, db_name, db_url, db_user, port, id_date)
