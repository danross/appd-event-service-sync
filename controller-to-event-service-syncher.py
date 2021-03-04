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
        self.__controller_client_id = Config.get("CONTROLLER", "api_client_name")
        self.__controller_client_secret = Config.get("CONTROLLER", "api_client_secret")


        self.__logger.debug("self.__controller_endpoint = " + str(self.__controller_endpoint))
        self.__logger.debug("self.__controller_account = " + str(self.__controller_account))
        self.__logger.debug("self.__controller_client_id = " + str(self.__controller_client_id))
        self.__logger.debug("self.__controller_client_secret = " + str(self.__controller_client_secret))

        self.__event_service_endpoint = Config.get("EVENT-SERVICE", "endpoint")
        self.__event_service_account = Config.get("EVENT-SERVICE", "global_account_name")
        self.__event_service_token = Config.get("EVENT-SERVICE", "api_key")

        self.__logger.debug("self.__event_service_endpoint = " + str(self.__event_service_endpoint))
        self.__logger.debug("self.__event_service_account = " + str(self.__event_service_account))
        self.__logger.debug("self.__event_service_token = " + str(self.__event_service_token))


        self.__event_service_headers = {"X-Events-API-AccountName" : self.__event_service_account, 
        "X-Events-API-Key" : self.__event_service_token, 
        "Content-type" :  "application/vnd.appd.events+json;v=2"}


        self.__controller_headers = {}
        self.generate_controller_token()

        self.__logger.debug("self.__controller_headers = " + str(self.__controller_headers)[:30] + "..." + str(self.__controller_headers)[-30:])

    def getControllerEndpoint(self):
        return self.__controller_endpoint

    def getEventServiceEndPoint(self):
        return self.__event_service_endpoint

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

    def get_metric(self, applicationname, metricpath, starttime, endtime):
        url = "/controller/rest/applications/" + str(applicationname) + "/metric-data"

        parameter = {"metric-path" : metricpath, "time-range-type" : "BETWEEN_TIMES", "start-time" : starttime, "end-time" : endtime, "output" : "json", "rollup" : "false"}
        r = self.get_from_controller(url, {}, parameter)

        try:
            json = r.json()
        except:
            self.__logger.error("Didnt received JSON from server: " + str(r))
            self.__logger.error("get_metric.response = " + str(r))
            self.__logger.error("get_metric.response.content = " + str(r.content))
            self.__logger.error("url = " + str(url))
            self.__logger.error("parameter = " + str(parameter))
            
            return ""
        
        if len(json) == 0:            
            self.__logger.debug("Empty response: " + str(r))

            return ""
        
        metric_list = []    

        json = json[0]
        metricValues = json["metricValues"]

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

        response = requests.get(url, headers=self.__controller_headers, params=parameter, data=data, verify=False)
        self.__logger.debug("GET " + str(url) + "\tdata = " + str(data) + "\tparameter = " + str(parameter) + "\t\tresponse:" + str(response))
        return response
        
    def post_to_eventservice(self, url, data, parameter):
        url = self.__event_service_endpoint + url
        #self.__logger.debug("POST " + str(url))

        return requests.post(url, headers=self.__event_service_headers, params=parameter, json=data, verify=False)

#End Controller class

class TimeRange:

    def __init__(self, start_time, end_time, sched_time, logger):
        self.__logger = logger
        self.__sched_time = sched_time

        if ":" in str(start_time):
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            self.__unix_start_time = TimeRange.datetime_to_unix_timestamp(start_time)
            self.__iso_start_time = start_time
        else:
            self.__unix_start_time = start_time
            self.__iso_start_time = TimeRange.unix_timestamp_to_date(start_time)


        if ":" in str(end_time):
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            self.__unix_end_time = TimeRange.datetime_to_unix_timestamp(end_time)
            self.__iso_end_time = end_time
        else:
            self.__unix_end_time = end_time
            self.__iso_end_time = TimeRange.unix_timestamp_to_date(end_time)


    def getUnixStartTimeInSeconds(self): return self.__unix_start_time
    def getUnixEndTimeInSeconds(self):   return self.__unix_end_time

    def getUnixStartTimeInMS(self): return self.__unix_start_time*1000
    def getUnixEndTimeInMS(self): return self.__unix_end_time*1000

    def getISOStartTime(self): return self.__iso_start_time
    def getISOEndTime(self): return self.__iso_end_time
    
    def IncreaseRangeBySchedTime(self):
        self.IncreaseRangeBySeconds(self.__sched_time)

    def IncreaseRangeBySeconds(self, seconds):
        self.__unix_start_time = self.__unix_start_time + seconds
        self.__unix_end_time = self.__unix_end_time + seconds

        self.__iso_start_time = self.unix_timestamp_to_date(self.__unix_start_time)
        self.__iso_end_time = self.unix_timestamp_to_date(self.__unix_end_time)

    @staticmethod
    def unix_timestamp_to_date(epochtime):
        epochtime = int(epochtime)

        formatted = datetime.datetime.fromtimestamp(epochtime)
        return formatted
    @staticmethod
    def datetime_to_unix_timestamp(dt):

        timestamp = dt.timestamp()
        return int(timestamp)


def post_metric_entry_to_event_service(metrics, name, application, metricpath, ctrl):


    for metric in metrics:

        startTimeInMillis = int(metric["startTimeInMillis"])

        data = {
            "metricname" : str(name),
            "application" : application,
            "startTimeInMillis" : startTimeInMillis,
            "metricpath" : metricpath,
            "value" : int(metric["value"]) 
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

    all_controller_apps = ctrl.get_applications()

    result = []
    for app in Config.sections():
        print("app = '" + str(app) + "'")
        print("all_controller_apps = " + str(all_controller_apps))
        if app == "ALL_APPLICATIONS":
            print("In case: ALL_APPLICATIONS")
            for el in all_controller_apps:
                el = el["name"]
                
                for key, val in Config.items(app):
                    result.append((el,key,val))
        elif app.startswith("CONTAINS:"):
            contains = app[len("CONTAINS:"):]
            print("found CONTAINS in app name config")

            for el in all_controller_apps:
                el = el["name"]
                if contains in el:
                    print("\ttrue")
                    for key, val in Config.items(app):
                        result.append((el,key,val))
        else:
            for key, val in Config.items(app):
                result.append((app,key,val))
        

    return result



def process_all_metrics(controller, timerange, metrics_to_process, appd_agent_enabled=False):

    if appd_agent_enabled: process_all_metrics_bt = appd.start_bt('process_all_metrics')
    
    def pull_and_push_metric(controller, targetMetricName, app, path, starttime,endtime, appd_agent_enabled):

        path = path.replace("\"","")

        if appd_agent_enabled: controller_exit_call = appd.start_exit_call(bt_handle=process_all_metrics_bt, exit_type=appd.EXIT_HTTP, display_name="Controller", identifying_properties={"host" : ctrl.getControllerEndpoint()}, operation="get_metrics")
        metrics = ctrl.get_metric(app,path,starttime,endtime)
        if appd_agent_enabled: appd.end_exit_call(controller_exit_call)

        if appd_agent_enabled:
            appd.add_snapshot_data(process_all_metrics_bt, "application", app)
            appd.add_snapshot_data(process_all_metrics_bt, "path", path)

            #appd.add_snapshot_data(process_all_metrics_bt, "starttime", TimeRange.unix_timestamp_to_date(starttime)/1000)
            #appd.add_snapshot_data(process_all_metrics_bt, "endtime", TimeRange.unix_timestamp_to_date(endtime)/1000)


        if (metrics != None and len(metrics) > 0):
            if appd_agent_enabled: event_service_exit_call = appd.start_exit_call(bt_handle=process_all_metrics_bt, exit_type=appd.EXIT_HTTP, display_name="Event-service", identifying_properties={"host" : ctrl.getEventServiceEndPoint()}, operation="push_metrics")
            if appd_agent_enabled:
                appd.add_snapshot_data(process_all_metrics_bt, "metrics", metrics)
                appd.add_snapshot_data(process_all_metrics_bt, "targetMetricName", targetMetricName)
            post_metric_entry_to_event_service(metrics, targetMetricName, app, path, ctrl)
            if appd_agent_enabled: appd.end_exit_call(event_service_exit_call)

            return 1

        return 0

    start_datetime = timerange.getUnixStartTimeInMS()
    end_datetime = timerange.getUnixEndTimeInMS()
    print("\nProcessing metrics from " + str(timerange.getISOStartTime()) + " to " + str(timerange.getISOEndTime()))
    start = datetime.datetime.now()
    metric_received = 0

    futures = []

    with concurrent.futures.ThreadPoolExecutor(100) as executor:
        for app,targetMetricName,path in metrics_to_process:
            future = executor.submit(pull_and_push_metric,controller, targetMetricName, app, path, start_datetime, end_datetime, appd_agent_enabled)
            futures.append(future)
            

        for f in concurrent.futures.as_completed(futures):
            metric_received = metric_received + f.result()
            
    print("metric_received = " + str(metric_received))


    end = datetime.datetime.now()
    difference = end - start
    printString = ("Needed " + str(difference.total_seconds()) + " seconds to process the metrics")
    printString = printString + "\n\t" + str(start).split(":")[-1] + " - " + str(end).split(":")[-1]
    print(printString)

    timerange.IncreaseRangeBySchedTime()
    if appd_agent_enabled: appd.end_bt(process_all_metrics_bt)

#Konstanten
ALL_APPS = "ALL_APPLICATIONS"

def printHelp():
    print("\tBenutze --schema=MeinSchema um in ein anderes Schema im Eventservice zu schreiben. Der Standard ist custom_metrics")
    print("\tBenutze --metricFileName=MeineDatei um die Metric config aus einer anderen Datei zu lesen. Der Standard ist metrics.txt")
    print("\tBenutze --startTime=\"2020-12-31 11:37:00\" und --endTime=\"2020-12-31 15:00:00\" um einen fest definierten Zeitraum zu synchronisieren. Danach beendet sich die Applikation")
    #print("\tBenutze --schedule_time=60 um die Abtastrate (wie oft werden Daten abgerufen) in Sekunden zu definieren in . Der Standard ist 60.")
    #print("\tBenutze --minutes_delay=1 um den Delay der Abrufe zu konfigurieren. Ein minutes_delay von 1 bedeutet, dass um 15:00 Uhr die Daten von 14:59 abgerufen werden. \n\tWenn der Wert zu klein ist, kann es passieren, dass die Daten noch nicht da sind. Standard ist 3")
    #print("\tBenutze --logging_level=DEBUG/INFO um den logging Level zu setzen")
    
    sys.exit(-1)

if __name__ == "__main__":
    print("sys.argv = " + str(sys.argv))

    #Sollte über einen Parameter gesetzt werden können
    metric_file_name = "metrics.txt"
    schema_name = "custom_metrics"
    start_time = "-1"
    end_time = "-1"
    sched_time = 60
    minutes_delay = 3
    logging_level = "INFO"
    

    stop = False
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
    


    Config = configparser.ConfigParser()
    Config.read("config.txt")

    sched_time = int(Config.get("SYNCER-CONFIG", "schedule_time"))
    minutes_delay = int(Config.get("SYNCER-CONFIG", "minutes_delay"))
    logging_level = Config.get("SYNCER-CONFIG", "logging_level")

    appd_agent_enabled = Config.getboolean("SYNCER-CONFIG", "Python_agent")

    if appd_agent_enabled:
        from appdynamics.agent import api as appd

        logging.info("Import appdynamics agent SDK")
        #timeout_ms = None --> Die Initialisierung erfolgt synchron, bis der Agent seine Konfiguration vom Controller erhalten hat
        init = appd.init(environ = None, timeout_ms = None)

        if not init:
            logging.error("Pyagent ist nicht richtig konfiguriert")


    logger = logging.getLogger("SYNCHER")

    if logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "INFO":
        logger.setLevel(logging.INFO)
    else:
        print("Logging level not defined! Abort")
        sys.exit(-1)


    fh = logging.FileHandler('syncher.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("schema_name = " + str(schema_name))
    logger.info("metricFileName = " + str(metric_file_name))
    logger.info("startTime = " + str(start_time))
    logger.info("endTime = " + str(end_time))
    logger.info("schedule_time = " + str(sched_time))
    logger.info("minutes_delay = " + str(minutes_delay))

    time_frame_error = ""
    if (start_time != "-1") and (end_time == "-1"):
        s = "Parameterfehler: startTime gesetzt, aber endTime nicht"
        print(s)
        logger.error(s)
        sys.exit(-1)

    if (start_time == "-1") and (end_time != "-1"):
        s = "Parameterfehler:  startTime nicht gesetzt, aber endTime"
        print(s)
        logger.error(s)
        sys.exit(-1)

    now = datetime.datetime.now()
    now = now.replace(second=0, microsecond=0)
    now = now - datetime.timedelta(minutes=minutes_delay)


    if not(start_time == "-1") and not(end_time == "-1"):
        tr = TimeRange(start_time, end_time,minutes_delay, logger)

        start_iso_date = tr.getISOStartTime()
        end_iso_date = tr.getISOEndTime()

        if start_iso_date > end_iso_date:
            s = "startDate ist später als endDate"
            print(s)
            logger.error(s)
            sys.exit(-1)

        if start_iso_date > now:
            s = "start_date ist in der Zukunft"
            print(s)
            logger.error(s)
            sys.exit(-1)

    logger.info("Use Schema: '" + str(schema_name) + "'")

    if appd_agent_enabled: init_controller_and_event_service = appd.start_bt('init_controller_and_event_service')
    ctrl = Controller(logger)
    if appd_agent_enabled: appd.end_bt(init_controller_and_event_service)



    logger.info("Read metrics from: '" + str(metric_file_name) + "'")

    if appd_agent_enabled: read_metric_bt = appd.start_bt('read_metric_config')
    metrics_to_process = read_metric_config(metric_file_name)
    if appd_agent_enabled: appd.end_bt(read_metric_bt)


    logger.debug("metrics_to_process = " + str(metrics_to_process))
    logger.debug("len(metrics_to_process) = " + str(len(metrics_to_process)))

    #In diesem Fall wird nur ein bestimmter Zeitraum verarbeitet    
    if not(start_time == "-1") and not(end_time == "-1"):
        print("Only sync limited time range")
        process_all_metrics(ctrl, tr, metrics_to_process=metrics_to_process, appd_agent_enabled=appd_agent_enabled)
        
    else:
        start_time = now.strftime("%Y-%m-%d %H:%M:%S")
        end_time = now + datetime.timedelta(minutes=1)
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

        tr = TimeRange(start_time, end_time,sched_time, logger)


        process_all_metrics(controller=ctrl, timerange=tr,  metrics_to_process=metrics_to_process, appd_agent_enabled=appd_agent_enabled)
        schedule.every(sched_time).seconds.do(job_func=process_all_metrics, controller=ctrl, timerange=tr,  metrics_to_process=metrics_to_process, appd_agent_enabled=appd_agent_enabled)
        
        while True:
            schedule.run_pending()

