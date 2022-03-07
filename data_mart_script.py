import logging
import sys
import threading

from analysis import analysis_helper
from api.twitter_helper import TwitterHelper
from data.dataset import Dataset
from helpers.datalake_helper import DatalakeHelper
from helpers.datamart_helper import DataMartHelper
from helpers.datawarehouse_helper import DataWarehouseHelper
from datetime import date, datetime

from database.local_db_helper import LocalDBHelper


def log(message, level):
    if True:
        print(message)
    if level == "INFO":
        logging.info(message)
    elif level == "ERROR":
        logging.error(message)


def processing(alias, db_name, db_url, db_user, port, id_date):
    try:
        message = "Start data transfer from DWH to DM [source : TWITTER, client : " + alias + ", date : " + date + "]"
        log(message, "INFO")

        datamart_helper = DataMartHelper(alias, db_name, db_url, db_user, port)
        datamart_helper.insert_fact_record_twitter(id_date)
        datamart_helper.insert_fact_frequency(id_date)
    except FileExistsError as e:
        message = "Error occurred while transferring data from DWH to DM [source : TWITTER, client : " + alias + ", date : " + date + "] -> " + str(
            e)
        log(message, "ERROR")
        return


logfile = "log/DM_" + str(date.today()) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s (%(name)s) ->  %(levelname)s :: %(message)s')

try:
    local_db_helper = LocalDBHelper()
    dwh_helper = DataWarehouseHelper()
except Exception as e:
    message = "Error occurred during Fetching Helpers initialization : " + str(e)
    log(message, "ERROR")
    sys.exit(-1)

date = datetime.today().strftime('%Y-%m-%d')
id_date = dwh_helper.get_id_date(date)
threads = []
for client in local_db_helper.get_active_clients():
    alias = client[1]
    db_name = client[4]
    db_url = client[5]
    db_user = client[6]
    port = client[7]
    if alias == "NETFLIX":
        continue
    threads.append(threading.Thread(target=processing, args=(alias, db_name, db_url, db_user, port, id_date,)))

for t in threads:
    t.start()

for t in threads:
    t.join()
