import base64
import sys
import logging
import schedule
import time
import datetime
import threading

import requests
import json
import tempfile, os, shutil
from xml.etree import ElementTree as ET
import configparser
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import concurrent.futures


class Controller: 

    """ Manage the operations on the controller """
    def __init__(self, logger):

        self.__logger = logger
        self.__token_expiry_time = 0

        Config = configparser.ConfigParser()
        Config.read("config.txt")

        self.__controller_endpoint = Config.get("CONTROLLER", "endpoint")
        self.__controller_global_ac_name = Config.get("CONTROLLER", "global_acc_name")
        self.__controller_access_key = Config.get("CONTROLLER", "access_key")


        self.__logger.debug("self.__controller_endpoint = " + str(self.__controller_endpoint))
        self.__logger.debug("self.__controller_global_ac_name = " + str(self.__controller_global_ac_name))
        self.__logger.debug("self.__controller_access_key = " + str(self.__controller_access_key))

        self.__event_service_endpoint = Config.get("EVENT-SERVICE", "endpoint")

        self.__logger.debug("self.__event_service_endpoint = " + str(self.__event_service_endpoint))


        self.__event_service_account = Config.get("CONTROLLER", "global_acc_name")
        self.__event_service_token = Config.get("EVENT-SERVICE", "api_key")

        self.__event_service_headers = {"X-Events-API-AccountName" : self.__event_service_account, 
        "X-Events-API-Key" : self.__event_service_token, 
        "Content-type" :  "application/vnd.appd.events+json;v=2"}

    def post_to_eventservice_using_basic_auth(self, url, data, parameter):
        url = self.__event_service_endpoint + url

        auth_string = self.__controller_global_ac_name + ":" + self.__controller_access_key
        auth_string = auth_string.encode()
        auth_string = base64.b64encode(auth_string)

        h = {}
        h["Content-type"] = "application/vnd.appd.events+json;v=2"
        h["Accept"] = "application/json"
        h["Authorization"] = b"Basic " + auth_string


        return requests.post(url, headers=h, params=parameter, data=data, verify=False)

    def get_from_eventservice_using_basic_auth(self, url, data, parameter):
        url = self.__event_service_endpoint + url

        auth_string = self.__controller_global_ac_name + ":" + self.__controller_access_key
        auth_string = auth_string.encode()
        auth_string = base64.b64encode(auth_string)

        h = {}
        h["Content-type"] = "application/vnd.appd.events+json;v=2"
        h["Accept"] = "application/json"
        h["Authorization"] = b"Basic " + auth_string


        return requests.get(url, headers=h, params=parameter, data=data, verify=False)

    def post_to_eventservice(self, url, data, parameter):
        url = self.__event_service_endpoint + url
        #self.__logger.debug("POST " + str(url))

        return requests.post(url, headers=self.__event_service_headers, params=parameter, data=data, verify=False)



def printHelp():
    print("\tBenutze --QueryToDelete='select * from ...'  um die Daten einer bestimmten Query zu löschen.")
    
    sys.exit(-1)

if __name__ == "__main__":
    if len(sys.argv) == 1: printHelp()

    
    query = ""
    for arg in sys.argv:
        #key value pair
        if arg[:2] == ("--"):
            arg = arg[2:]
            key,value = arg.split("=", 1)
            if key == "QueryToDelete": query = value
        elif arg[:1] == "-":
            arg = arg[1:]
            if arg == "help" : printHelp()


    Config = configparser.ConfigParser()
    Config.read("config.txt")
    logging_level = Config.get("CONFIG", "logging_level")
    logger = logging.getLogger("query-deleter")

    if logging_level == "DEBUG":
        logging.info("logger.setLevel(logging.DEBUG)")
        logger.setLevel(logging.DEBUG)
    elif logging_level == "INFO":
        logging.info("logger.setLevel(logging.INFO)")
        logger.setLevel(logging.INFO)
    else:
        logging.error("Logging level nicht definiert. Setze das Logging Level im Abschnitt [CONFIG] über den Parameter logging_level=INFO/DEBUG")
        sys.exit(-1)


    fh = logging.FileHandler('deleter.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())



    ctrl = Controller(logger)
    logger.info("\n\nQuery ausgewählt:\t'" + str(query) + "'")

    paras = {"start" : "1604222220000", "end" : "1606735020000"}
    response = ctrl.post_to_eventservice(url="/events/query", data=query, parameter=paras)

    json = response.json()
    json = json[0]
    total = json["total"]
    print("Zeilen die gelöscht werden sollen: " + str(total))
    confirm = input("\nBitte bestätige die Anzahl von Zeilen die gelöscht werden sollen: ")
    print("confirm = " + str(confirm))

    if not(str(confirm) == str(total)):
        print("Bestätigung falsch - breche ab")
        import sys; sys.exit(-1)
    else:
        print("Bestätigung richtig.")

    response = ctrl.post_to_eventservice_using_basic_auth(url="/events/query/_delete", data=query, parameter=paras)

    try:
        json = response.json()
        statusUrl = json["statusUrl"]

        resp = ctrl.get_from_eventservice_using_basic_auth(url=statusUrl, data={}, parameter={})
        json = resp.json()
        print("json = " + str(json))
        status = json["statusCode"]
        totalFound = json["details"]["TotalFound"]
        totalFound = int(totalFound)

        logging.info("status = " + str(status))
        while not(status == "COMPLETED"):
            resp = ctrl.get_from_eventservice_using_basic_auth(url=statusUrl, data={}, parameter={})
            json = resp.json()
            status = json["statusCode"]

            totalDeleted = json["details"]["TotalDeleted"]
            totalDeleted = int(totalDeleted)
            progress = totalDeleted / totalFound
            print(str(progress), end="\t")

        
        statuscode = json["statusCode"]
        totalDeleted = json["details"]["TotalDeleted"]

        logging.info("\nStatusCode = " + str(statuscode))
        logging.info("Habe " + totalDeleted + " Einträge von " + str(totalFound) + " gelöscht.")
    except:
        logging.warning("Status konnte nicht abgerufen werden. Erneute Abfrage der Query.")
    
        response = ctrl.post_to_eventservice(url="/events/query", data=query, parameter=paras)

        json = response.json()
        json = json[0]
        total = json["total"]
        if str(total) == "0":
            print("Löschung war erfolgreich")
        else:
            print("Nach Löschung sind noch '" + str(total) + "' Einträge vorhanden. Löschung war nicht erfolgreich.")

        print("Zeilen die gelöscht werden sollen: " + str(total))
