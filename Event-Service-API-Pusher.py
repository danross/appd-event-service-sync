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

    def create_schema(self, name, schema, re_crease_if_exists = False):
        url = "/events/schema/" + str(name)
        response = self.post_to_eventservice(url=url, data=schema, parameter={})

        if (response.status_code == 409 and re_crease_if_exists):
            self.__logger.info("deleting schema")
            resp = self.delete_schema(name)
            response = self.post_to_eventservice(url=url, data=schema, parameter={})

        return response

    def delete_schema(self, name):
        url = "/events/schema/" + str(name)
        url = self.__event_service_endpoint + url

        req = requests.delete(url, headers=self.__event_service_headers, verify=False)
        return req
        

    def get_metric(self, applicationname, metricpath, starttime, endtime):
        url = "/controller/rest/applications/" + str(applicationname) + "/metric-data"

        parameter = {"metric-path" : metricpath, "time-range-type" : "BETWEEN_TIMES", "start-time" : starttime, "end-time" : endtime, "output" : "json", "rollup" : False}
        r = self.get_from_controller(url, {}, parameter)

        try:
            json = r.json()
        except:
            self.__logger.error("Didnt received JSON from server: " + str(r))
            return None       
        
        if len(json) == 0:            
            return None
        

        metric_list = []    

        json = json[0]
        metricValues = json["metricValues"]

        s = "parameter = " + str(parameter) + "\n"
        s = s + str(metricValues)
        print(s)

        return metricValues


    def get_applications(self):
        url = "/controller/rest/applications"
        response = self.get_from_controller(url, {}, {"output" : "JSON"})
        
        return response.json()

        
    
    def post_to_controller(self, url, data, parameter):
        url = self.__controller_endpoint + url
        #self.__logger.debug("POST " + str(url))

        return requests.post(url, headers=self.__controller_headers, params=parameter, data=data, verify=False)
        
        
    def get_from_controller(self, url, data, parameter):
        url = self.__controller_endpoint + url
        self.__logger.debug("GET " + str(url))

        return requests.get(url, headers=self.__controller_headers, params=parameter, data=data, verify=False)
        
    def post_to_eventservice(self, url, data, parameter):
        url = self.__event_service_endpoint + url
        #self.__logger.debug("POST " + str(url))

        return requests.post(url, headers=self.__event_service_headers, params=parameter, json=data, verify=False)

#End Controller class

class TimeRange:

    def __init__(self, start_time, end_time, logger):
        self.__logger = logger
        print("start_time = " + str(start_time))
        print("end_time = " + str(end_time))

        if ":" in str(start_time):
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            self.__unix_start_time = self.datetime_to_unix_timestamp(start_time)
            self.__iso_start_time = start_time
        else:
            self.__unix_start_time = start_time
            self.__iso_start_time = self.unix_timestamp_to_date(start_time)


        if ":" in str(end_time):
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            self.__unix_end_time = self.datetime_to_unix_timestamp(end_time)
            self.__iso_end_time = end_time
        else:
            self.__unix_end_time = end_time
            self.__iso_end_time = self.unix_timestamp_to_date(end_time)

        #print("getUnixStartTimeInSeconds = " + str(self.getUnixStartTimeInSeconds()))
        #print("getUnixEndTimeInSeconds = " + str(self.getUnixEndTimeInSeconds()))

        #print("getISOStartTime = " + str(self.getISOStartTime()))
        #print("getISOEndTime = " + str(self.getISOEndTime()))
        #import sys; sys.exit(-1)


    def getUnixStartTimeInSeconds(self): return self.__unix_start_time
    def getUnixEndTimeInSeconds(self):   return self.__unix_end_time

    def getUnixStartTimeInMS(self): return self.__unix_start_time*1000
    def getUnixEndTimeInMS(self): return self.__unix_end_time*1000

    def getISOStartTime(self): return self.__iso_start_time
    def getISOEndTime(self): return self.__iso_end_time
    

    def IncreaseRangeBySeconds(self, seconds):
        self.__unix_start_time = self.__unix_start_time + seconds
        self.__unix_end_time = self.__unix_end_time + seconds

        self.__iso_start_time = unix_timestamp_to_date(self.__unix_start_time)
        self.__iso_end_time = unix_timestamp_to_date(self.__unix_end_time)


    def unix_timestamp_to_date(self, epochtime):
        epochtime = int(epochtime)

        formatted = datetime.datetime.fromtimestamp(epochtime)
        return formatted

    def datetime_to_unix_timestamp(self, dt):

        timestamp = dt.timestamp()
        return int(timestamp)


def post_metric_entry_to_event_service(metrics, name, application, metricpath, ctrl):

    #print("metrics = " + str(metrics))
    #import sys; sys.exit(-1)

    for metric in metrics:

        startTimeInMillis = int(metric["startTimeInMillis"])

        data = {
            "metricname" : str(name),
            "application" : application,
            "startTimeInMillis" : startTimeInMillis,
            "metricpath" : metricpath,
            "value" : str(metric["value"]) 
            }

        
        metricpath_parts = metricpath.split("|")
        
        for i in range(len(metricpath_parts)):
            data["metricpath"+str(i+1)] = metricpath_parts[i]

        #print("data = " + str(data))
        
        url = "/events/publish/" + str(schema_name)
        logger.debug("push to event-service: " + str(data))
        resp = ctrl.post_to_eventservice(url, [data], {})
        try:
            resp = resp.json()
        except:
            None

        logger.debug("event-service response:" + str(resp))
        

def read_metric_config(filename):
    Config = configparser.ConfigParser()
    Config.read(filename)

    logger.debug("Config.sections() = " + str(Config.sections()))


    result = []
    for app in Config.sections():
        print("app = " + str(app))
        if app == "ALL_APPLICATIONS":
            #print("Found section: ALL_APPLICATIONS")

            all_controller_apps = ctrl.get_applications()
            for el in all_controller_apps:
                el = el["name"]
                
                for key, val in Config.items(app):
                    result.append((el,key,val))
        else:
            for key, val in Config.items(app):
                result.append((app,key,val))
        

    return result



def process_all_metrics(controller, timerange, metrics_to_process):

    def pull_and_push_metric(controller, targetMetricName, app, path, starttime,endtime):
        path = path.replace("\"","")


        #s = ""
        #s = s + ("app = " + str(app)) + "\n"
        #s = s + ("path = " + str(path)) + "\n"
        #s = s + ("starttime = " + str(starttime)) + "\n"
        #s = s + ("endtime = " + str(endtime)) + "\n"

        metrics = ctrl.get_metric(app,path,starttime,endtime)
        #s = s + ("metrics = " + str(metrics)) + "\n\n\n"

        #print(s)
        #import sys; sys.exit(-1)
        if (metrics != None and len(metrics) > 0):
            post_metric_entry_to_event_service(metrics, targetMetricName, app, path, ctrl)



    start_datetime = timerange.getUnixStartTimeInMS()
    end_datetime = timerange.getUnixEndTimeInMS()
    print("\nProcessing metrics from " + str(timerange.getISOStartTime()) + " to " + str(timerange.getISOEndTime()))
    start = datetime.datetime.now()
    metric_received = 0

    threads = []

    for app,targetMetricName,path in metrics_to_process:
        metric_thread = threading.Thread(target=pull_and_push_metric, args=(controller, targetMetricName, app, path, start_datetime, end_datetime))
        threads.append(metric_thread)
    
    for metric_thread in threads:
        metric_thread.start()

    end = datetime.datetime.now()
    difference = end - start
    printString = ("Needed " + str(difference.total_seconds()) + " seconds to process the metrics (Running threads: " + str(threading.active_count()) + ")")
    printString = printString + "\n\t" + str(start).split(":")[-1] + " - " + str(end).split(":")[-1]

    print(printString)



#Konstanten
ALL_APPS = "ALL_APPLICATIONS"
schema = {"metricname" : "string", "application" : "string",
    "metricpath"  : "string", "metricpath1" : "string", "metricpath2" :  "string", "metricpath3"  : "string",
    "metricpath4" : "string", "metricpath5" : "string", "metricpath6" :  "string", "metricpath7"  : "string",
    "metricpath8" : "string", "metricpath9" : "string", "metricpath10" : "string", "metricpath11" : "string",
    "startTimeInMillis" : "Date", 
    "application" : "string",
    "value" : "string"}

def printHelp():
    print("Benutze --schema=MeinSchema um in ein anderes Schema im Eventservice zu schreiben. Der Standard ist custom_metrics")
    print("Benutze --metricFileName=MeineDatei um die Metric config aus einer anderen Datei zu lesen. Der Standard ist metrics.txt")
    print("Benutze --startTime=2020-12-31 11:37:00 und --endTime=2020-12-31 15:00:00 um einen fest definierten Zeitraum zu synchronisieren. Danach beendet sich die Applikation")
    sys.exit(-1)

if __name__ == "__main__":
    print("sys.argv = " + str(sys.argv))

    logger = logging.getLogger("app")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('syncher.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    fh.setFormatter(formatter)

    logger.addHandler(fh)

    
    #Sollte über einen Parameter gesetzt werden können
    metric_file_name = "metrics.txt"
    schema_name = "custom_metrics"

    start_time = "-1"
    end_time = "-1"

    for arg in sys.argv:
        #key value pair
        if arg[:2] == ("--"):
            arg = arg[2:]
            key,value = arg.split("=")
            if key == "schema": schema_name = value
            if key == "metricFileName" : metric_file_name = value
            if key == "startTime" : start_time = value
            if key == "endTime" : end_time = value
        elif arg[:1] == "-":
            arg = arg[1:]
            if arg == "help" : printHelp()

    

    logger.info("Read metrics from: '" + str(metric_file_name) + "'")
    logger.info("Use Schema: '" + str(schema_name) + "'")

    ctrl = Controller(logger)
    ctrl.create_schema(schema_name , {"schema" : schema}, re_crease_if_exists=True)
    metrics_to_process = read_metric_config(metric_file_name)

    logger.debug("len(metrics_to_process) = " + str(len(metrics_to_process)))
    logger.debug("metrics_to_process = " + str(metrics_to_process))


    now = datetime.datetime.now()
    now = now.replace(second=0, microsecond=0)
    now = now - datetime.timedelta(minutes=1)



    #In diesem Fall wird nur ein bestimmter Zeitraum verarbeitet    
    if not(start_time == "-1") and not(end_time == "-1"):

        print("Only sync limited time range")
        tr = TimeRange(start_time, end_time, logger)
        process_all_metrics(ctrl, tr, metrics_to_process=metrics_to_process)
        
    else:
        #run_threaded(process_all_metrics, controller=ctrl, metrics_to_process=metrics_to_process)
        #schedule.every(60).seconds.do(job_func=run_threaded,func_to_call=process_all_metrics, controller=ctrl, metrics_to_process=metrics_to_process)
        

        logger.info("Start processing for minute: " + str(now))
        current_epoch_time = datetime_to_unix_timestamp(now)
        logger.info("current_epoch_time = " + str(current_epoch_time))



        #process_all_metrics(ctrl, starttime = current_epoch_time, metrics_to_process)
        #current_epoch_time = current_epoch_time + 60


        #schedule.every(60).seconds.do(job_func=process_all_metrics,ctrl, current_epoch_time, metrics_to_process)



        while True:
            schedule.run_pending()

