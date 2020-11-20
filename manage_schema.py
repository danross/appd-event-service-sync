#from appdynamics.agent import api as appd
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
        self.__controller_account = Config.get("CONTROLLER", "account")
        self.__controller_client_id = Config.get("CONTROLLER", "client_id")
        self.__controller_client_secret = Config.get("CONTROLLER", "client_secret")


        self.__logger.debug("self.__controller_endpoint = " + str(self.__controller_endpoint))
        self.__logger.debug("self.__controller_account = " + str(self.__controller_account))
        self.__logger.debug("self.__controller_client_id = " + str(self.__controller_client_id))
        self.__logger.debug("self.__controller_client_secret = " + str(self.__controller_client_secret))

        self.__event_service_endpoint = Config.get("EVENT-SERVICE", "endpoint")
        self.__event_service_account = Config.get("EVENT-SERVICE", "accountname")
        self.__event_service_token = Config.get("EVENT-SERVICE", "token")

        self.__logger.debug("self.__event_service_endpoint = " + str(self.__event_service_endpoint))
        self.__logger.debug("self.__event_service_account = " + str(self.__event_service_account))
        self.__logger.debug("self.__event_service_token = " + str(self.__event_service_token))


        self.__event_service_headers = {"X-Events-API-AccountName" : self.__event_service_account, 
        "X-Events-API-Key" : self.__event_service_token, 
        "Content-type" :  "application/vnd.appd.events+json;v=2"}


        self.__controller_headers = {}
        self.generate_controller_token()

        self.__logger.debug("self.__controller_headers = " + str(self.__controller_headers)[:30] + "..." + str(self.__controller_headers)[-30:])

    #Controller OAUTH
    def generate_controller_token(self):    
        url = self.__controller_endpoint + "/controller/api/oauth/access_token"

        d = {"grant_type" : "client_credentials", 
            "client_id" : self.__controller_client_id+"@"+self.__controller_account,
            "client_secret" : self.__controller_client_secret}

        req = requests.post(url, data=d, verify=False)
        self.__logger.debug("response from token_auth: " + str(req.reason))
        try:
            data = req.json()
            token = data["access_token"]
            expires_in = data["expires_in"]

            self.__token_expiry_time = expires_in
            self.__controller_headers = {"Authorization" : "Bearer " + str(token)}

            if expires_in < 15:
                self.__logger.warning("Token laeuft in weniger als 15 Sekunden ab")

            
            self.__logger.info("Token expires in " + str(expires_in) + " seconds")
        except:
            self.__logger.error("Failed to receive OAUTH token")

        #Entferne den schedule mit der alten Expiry Time (Der neue Token hat nicht notwendig die gleiche Expiry Time wie der alte)
        schedule.clear('oauth-token-tasks')
        schedule.every(expires_in-10).seconds.do(job_func=self.generate_controller_token).tag('oauth-token-tasks')

    def create_schema(self, name, schema):
        url = "/events/schema/" + str(name)
        response = self.post_to_eventservice(url=url, data=schema, parameter={})

        if (response.status_code == 409):
            self.__logger.error("Schema does allready exists! Abort")
            sys.exit(-1)

        return response

    def delete_schema(self, name):
        url = "/events/schema/" + str(name)
        url = self.__event_service_endpoint + url

        req = requests.delete(url, headers=self.__event_service_headers, verify=False)
        return req
        
    def post_to_eventservice(self, url, data, parameter):
        url = self.__event_service_endpoint + url
        self.__logger.debug("POST " + str(url))

        return requests.post(url, headers=self.__event_service_headers, params=parameter, json=data, verify=False)


#Konstanten
schema = {"metricname" : "string", "application" : "string",
    "metricpath"  : "string", "metricpath1" : "string", "metricpath2" :  "string", "metricpath3"  : "string",
    "metricpath4" : "string", "metricpath5" : "string", "metricpath6" :  "string", "metricpath7"  : "string",
    "metricpath8" : "string", "metricpath9" : "string", "metricpath10" : "string", "metricpath11" : "string",
    "startTimeInMillis" : "Date", 
    "application" : "string",
    "value" : "string"}

def printHelp():
    print("\tBenutze --schema=MeinSchema um ein Schema auszuwählen. Der Parameter ist nicht optional.")
    print("\tBenutze --operation=create/delete um ein Schema zu erstellen oder zu löschen. Der Parameter ist nicht optional.")

    
    sys.exit(-1)

if __name__ == "__main__":
    print("sys.argv = " + str(sys.argv))

    #Sollte über einen Parameter gesetzt werden können
    logging_level = "INFO"
    schema_name = "-1"
    operation = "-1"
    for arg in sys.argv:
        #key value pair
        if arg[:2] == ("--"):
            arg = arg[2:]
            key,value = arg.split("=")
            if key == "schema": schema_name = value
            if key == "operation": operation = value
        elif arg[:1] == "-":
            arg = arg[1:]
            if arg == "help" : printHelp()

    logger = logging.getLogger("SYNCHER")

    if logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

        

    fh = logging.FileHandler('schema_management.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())

    ctrl = Controller(logger)

    if schema_name == "-1":
        logger.error("No schema selected! Abort")
        sys.exit(-1)

    logger.info("Use Schema: '" + str(schema_name) + "'")

    if operation == "-1":
        logger.error("No operation selected! Abort")
        sys.exit(-1)

    confirm = input("\nPlease type in the operation you have selected: ")

    if not (confirm == operation):
        logger.error("Confirmation is wrong! Abort")
        sys.exit(-1)


    if operation == "create":
        logger.info("Creating schema")
        response = ctrl.create_schema(schema_name, {"schema" : schema})
        logger.info("\tschema = " + str(schema))
        logger.info("\tresponse = " + str(response))
        logger.info("\tresponse.reason = " + str(response.reason))

    if operation == "delete":
        logger.info("Deleting schema")
        response = ctrl.delete_schema(schema_name)
        logger.info("\tresponse = " + str(response))
        logger.info("\tresponse.reason = " + str(response.reason))




