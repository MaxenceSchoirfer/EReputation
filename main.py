import logging
import sys
import threading

from analysis import global_helper
from api.twitter_helper import TwitterHelper
from data.dataset import Dataset
from helpers.datalake_helper import DatalakeHelper
from helpers.datamart_helper import DataMartHelper
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


def processing(client, alias, id_date):
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

    # -------------------------------------- FEED DATA MART ----------------------------------------------------------

    try:
        message = "Start data transfer from DWH to DM [source : TWITTER, client : " + alias + ", date : " + date + "]"
        log(message, "INFO")

        datamart_helper = DataMartHelper(alias)
        datamart_helper.insert_fact_record_twitter(id_date)
        datamart_helper.insert_fact_frequency(id_date)
    except Exception as e:
        message = "Error occurred while transferring data from DWH to DM [source : TWITTER, client : " + alias + ", date : " + date + "]" + str(
            e)
        log(message, "ERROR")
        return


logfile = "log/log_" + str(date.today()) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s (%(name)s) ->  %(levelname)s :: %(message)s')

try:
    local_db_helper = LocalDBHelper()
    twitter_helper = TwitterHelper()
    datalake_helper = DatalakeHelper()
    dwh_helper = DataWarehouseHelper()
except Exception as e:
    logging.error("Error occurred during Fetching Helpers initialization : " + str(e))
    sys.exit(-1)

date = "2022-21-02"
clients = {}
for client in local_db_helper.get_active_clients():
    clients[client[1]] = client[2]
print(clients)

id_date = dwh_helper.get_id_date(date)
threads = []
for alias, client in clients.items():
    threads.append(threading.Thread(target=processing, args=(client, alias, id_date,)))

for t in threads:
    t.start()

for t in threads:
    t.join()
