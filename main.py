import logging
import sys
import threading

from analysis import global_helper
from api.twitter_helper import TwitterHelper
from data.dataset import Dataset
from helpers.datalake_helper import DatalakeHelper
from helpers.datawarehouse_helper import DataWarehouseHelper
from datetime import date

from database.local_db_helper import LocalDBHelper

PRINT = True


def log(message, level):
    if level == "INFO":
        logging.info(message)
    elif level == "ERROR":
        logging.error(message)
    if PRINT:
        print(message)


def processing(client, alias):
    # --------------------------- API CALL ------------------------------------------------------
    try:
        message = "Start fetching data from API [source : TWITTER, client : " + alias + ", date : " + date + "]"
        log(message, "INFO")
        twitter_helper.generate_csv(client, alias)
    except Exception as e:
        message = "Error occurred during fetching data from API [source : TWITTER, client : " + alias + ", date : " + date + "]" + str(
            e)
        log(message, "ERROR")
        return

    # SET FILE ON DATA LAKE

    # ----------------------------------------  DOWNLOAD FILES LOCALLY -------------------------------------------------------------
    try:
        message = "Start fetching files from datalake [source : TWITTER, client : " + alias + ", date : " + date + "]"
        log(message, "INFO")

        datalake_helper.download_files(alias)
        files = datalake_helper.get_filenames(alias)
    except Exception as e:
        message = "Error occurred during fetching files from datalake [source : TWITTER, client : " + alias + ", date : " + date + "]" + str(
            e)
        log(message, "ERROR")
        return

    # ------------------------------------- ANALYSIS ------------------------------------------------------------------

    for file in files:
        try:
            message = "Start analysis [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "]"
            log(message, "INFO")
            dataset = Dataset(file, False)
            global_helper.analysis(dataset)
        except Exception as e:
            message = "Error occurred during analysis [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "]" + str(
                e)
            log(message, "ERROR")
            return

        try:
            message = "Start storage of analyse results [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "]"
            log(message, "INFO")
            dwh_helper.save_dataset_analysis(dataset)
        except Exception as e:
            message = "Error occurred during storage of analyse results  [file : " + file + ", source : TWITTER, client : " + alias + ", date : " + date + "]" + str(
                e)
            log(message, "ERROR")
            return


logfile = "log/log_" + str(date.today()) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s (%(name)s) ->  %(levelname)s :: %(message)s')

# clients = ["COCA"]  # get active users -> from ??? DWH bad practice
date = "2022-21-02"

try:
    local_db_helper = LocalDBHelper()
    twitter_helper = TwitterHelper()
    datalake_helper = DatalakeHelper()
    dwh_helper = DataWarehouseHelper()
except Exception as e:
    logging.error("Error occurred during Fetching Helpers initialization : " + str(e))
    sys.exit(-1)

clients = {}
for client in local_db_helper.get_active_clients():
    clients[client[1]] = client[2]
print(clients)

threads = []
for alias, client in clients.items():
    threads.append(threading.Thread(target=processing, args=(client, alias,)))

for t in threads:
    t.start()

for t in threads:
    t.join()
