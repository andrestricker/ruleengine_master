import pandas as pd
import mariadb
import json
import configparser
import uuid
import redis
import time
import sysconfig
import subprocess
import os
import psutil
import platform
import signal
from jinja2 import Environment, FileSystemLoader
import numpy as np
import logging
from dataclasses import dataclass
from ldap3 import Server, Connection, ALL

config = configparser.ConfigParser()
config.read('config.ini')


@dataclass
class Message:
    sender_id: str
    messagetype: str
    sendertype: str
    payload: dict
    last_seen: float


class returndata:
    def __init(self):
        pass

    def build_returndata(self, payload):
        return {"time": time.time(), "payload": payload}


class user:
    def __init__(self):
        self.username = ""
        self.token = ""
        self.expires = 0

    def authenticate(self, username, password):
        s = Server(config["LDAP"]["server"], port=int(config[
                   "LDAP"]["port"]), get_info=ALL)
        c = Connection(s, user="cn="+username+"," +
                       config["LDAP"]["dn_suffix"], password=password)
        if c.bind():
            self.username = username
            self.token = self.build_token()
            self.expires = time.time() + \
                int(config["Auth"]["token_expiry_seconds"])
            r = returndata.build_returndata(returndata, {
                                            "username": self.username, "token": self.token, "token_expires": self.expires})
            return r

    def build_token(self):
        return str(uuid.uuid4())

    def is_in_group(self, groupname):

        pass


class system:
    def __init__(self):
        self.rules = rules()
        self.comms = redis.Redis(host=self.rules.host, port=self.rules.port,
                                 db=self.rules.db, decode_responses=True)

        self.mydb = mariadb.connect(
            host=config["DB"]["host"],
            user=config["DB"]["user"],
            password=config["DB"]["password"],
            database=config["DB"]["database"]
        )

    def selftest(self):
        res = {}
        res["watchdogs"] = self.selftest_watchdog_list()
        res["redis_connection"] = self.selftest_redis()
        res["mysql_connection"] = self.selftest_mysql()
        res["system"] = self.get_system_status()
        return res

    def selftest_watchdog_list(self):
        results = []
        sql = "SELECT * FROM watchdogs"
        self.rules.mycursor.execute(sql)

        watchdogs = [dict((self.rules.mycursor.description[i][0], value)
                          for i, value in enumerate(row)) for row in self.rules.mycursor.fetchall()]
        if len(watchdogs) == 0:
            return []
        for watchdog in watchdogs:
            sql = "SELECT * FROM runners"
            self.rules.mycursor.execute(sql)

            runners = [dict((self.rules.mycursor.description[i][0], value)
                            for i, value in enumerate(row)) for row in self.rules.mycursor.fetchall()]
            watchdog["runners"] = runners
            results.append(watchdog)

        return results

    def selftest_redis(self):
        try:
            self.comms.ping()
        except:
            return False

        return True

    def selftest_mysql(self):
        if config["Generic"]["db_engine"] == "MariaDB":
            try:
                self.mydb = mariadb.connect(
                    host=config["DB"]["host"],
                    user=config["DB"]["user"],
                    password=config["DB"]["password"],
                    database=config["DB"]["database"]
                )
            except:
                return False

        return True

    def get_system_status(self, logservices=False, logconnections=False):
        systeminfo = {}
        config = configparser.ConfigParser()
        if os.path.isfile("agent.ini"):
            try:
                self.config.read('agent.ini')
            except:
                pass
            else:
                systeminfo["uuid"] = config['DEFAULT']["uuid"]

        uname = platform.uname()
        systeminfo["system"] = uname.system
        systeminfo["os"] = platform.system()
        systeminfo["machine"] = platform.machine()
        systeminfo["architecture"] = platform.architecture()
        systeminfo["platform"] = sysconfig.get_platform()
        systeminfo["name"] = uname.node
        systeminfo["os_release"] = uname.release
        systeminfo["os_version"] = uname.version
        systeminfo["machine"] = uname.machine
        systeminfo["processor"] = uname.processor

        # partitions
        partitions = psutil.disk_partitions()
        systeminfo["partitions"] = {}
        for partition in partitions:
            try:
                partition_usage = psutil.disk_usage(partition.mountpoint)
            except PermissionError:
                pass
            systeminfo["partitions"][str(partition.device)] = {
                "mountpoint": partition.mountpoint,
                "filesystem_type": partition.fstype,
                "total": partition_usage.total,
                "used": partition_usage.used,
                "available": partition_usage.total - partition_usage.used,
                "used_perc": partition_usage.used/(partition_usage.total/100),
                "available_perc": 100-partition_usage.used/(partition_usage.total/100)}

        ## memory ##
        svmem = psutil.virtual_memory()
        systeminfo["memory_total"] = svmem.total
        systeminfo["memory_available"] = svmem.available
        systeminfo["memory_used"] = svmem.used
        systeminfo["memory_available_perc"] = svmem.available/(100/svmem.total)
        systeminfo["memory_used_perc"] = svmem.used/(100/svmem.total)

        swap = psutil.swap_memory()
        systeminfo["swap_memory_total"] = swap.total
        systeminfo["swap_memory_available"] = swap.free
        systeminfo["swap_memory_used"] = swap.used
        systeminfo["swap_memory_available_perc"] = swap.free/(swap.total/100)
        systeminfo["swap_memory_used"] = swap.used/(swap.total/100)

        # network connections
        if logconnections:
            connections = psutil.net_connections(kind='inet')
            systeminfo["connections"] = []
            for connection in connections:
                systeminfo["connections"].append(connection)
            # network interfaces

        if_addrs = psutil.net_if_addrs()
        # print(json.dumps(psutil.net_if_stats()))
        systeminfo["network"] = []

        for interface_name, interface_addresses in if_addrs.items():
            t = {}
            t["name"] = interface_name

            for address in interface_addresses:
                if str(address.family) == 'AddressFamily.AF_INET':
                    t["address"] = address.address
                    t["netmask"] = address.netmask
                    t["broadcast"] = address.broadcast
                   # try:
                    #                        t["dns"] = socket.gethostbyaddr(
                    #                           address.address)
                   # except:
                   #     pass

            systeminfo["network"].append(t)
        # services
        if logservices:
            if uname.system.upper() == "WINDOWS":
                r = list(psutil.win_service_iter())
                systeminfo["services"] = []

                for service in r:
                    thisservice = {}
                    dictservice = service.as_dict()
                    thisservice["binpath"] = dictservice["binpath"]
                    thisservice["name"] = dictservice["name"]
                    thisservice["start_type"] = dictservice["start_type"]
                    thisservice["status"] = dictservice["status"]
                    thisservice["username"] = dictservice["username"]
                    systeminfo["services"].append(thisservice)

        # configuration
        systeminfo["services_logged"] = logservices
        systeminfo["connections_logged"] = logconnections
        return systeminfo


class master:
    def __init__(self):

        self.rules = rules()
        self.comms = redis.Redis(host=self.rules.host, port=self.rules.port,
                                 db=self.rules.db, decode_responses=True)

        self.mydb = mariadb.connect(
            host=config["DB"]["host"],
            user=config["DB"]["user"],
            password=config["DB"]["password"],
            database=config["DB"]["database"]
        )

        self.receiving_topic = "to_master_from_watchdog"
        #self.watchdog_list = []
        self.mydb.autocommit = True
        self.mycursor = self.mydb.cursor()
        self.user = "11111"

    def event_handler(self, raw_message):

        msg_obj = json.loads(raw_message["data"])
        #print("++++", msg_obj)
        msg = self.parse_message(msg_obj)

        if msg.sendertype == "watchdog":
            #print("-----", msg.payload)
            if "runners" in msg.payload:
                print(msg.payload["runners"])
                self.set_watchdog_state(
                    msg.sender_id, msg.payload["runners"], msg.last_seen)

    def set_watchdog_state(self, id, runners, last_seen):
        sql = "REPLACE INTO watchdogs (uuid,  last_seen, last_modified_user_uuid) VALUES (%s,%s,%s)"
        val = (id, last_seen, self.user)

        self.mycursor.execute(sql, val)
        self.mydb.commit()

        sql = "DELETE FROM runners WHERE watchdog_uuid='"+id+"'"

        self.mycursor.execute(sql)
        self.mydb.commit()

        for runner in runners:
            sql = "INSERT INTO runners (uuid, watchdog_uuid, pid, last_seen, state, payload, last_modified_user_uuid) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            val = (runner["id"], id, runner["pid"],
                   runner["last_seen"], runner["state"], json.dumps(runner["payload"]), self.user)
            self.mycursor.execute(sql, val)
            self.mydb.commit()
        # try:
        #    c = self.get_watchdog_index(id)
        # except ValueError:
        #    self.watchdog_list.append(
        #        {"id": id, "runners": runners, "last_seen": last_seen})
        # else:
        #    self.watchdog_list[c]["id"] = id
        #    self.watchdog_list[c]["runners"] = runners

    # def get_watchdog_index(self, id):
    #    for c, watchdog in enumerate(self.watchdog_list):
    #        if watchdog["id"] == id:
    #            return c
    #    raise ValueError("Watchdog not found")

    def test_rule(self, id):
        results = []
        rule_exit_code = 0
        sql = "SELECT * FROM rule_tests where is_valid=1 and is_deleted=0 and rule_uuid='"+id+"'"
        self.rules.mycursor.execute(sql)

        res = [dict((self.rules.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.rules.mycursor.fetchall()]
        if len(res) == 0:
            rule_exit_code = 1
        for test in res:
            expected_output = json.loads(test["expected_output"])
            input_data = json.loads(test["input_data"])
            res2 = self.execute_rule(id, input_data)

            result = res2["payload"]["rule_result"][0]["result"]
            results.append({"input": input_data, "expected_result": expected_output,
                           "actual_result": result, "passed": result == expected_output})

        res = {"rule_exit_code": rule_exit_code, "results": results}
        return res

    def execute_rule(self, id, data):
        try:
            ru = self.rules.read_rule(id)
        except Exception as e:
            return(str(e))
        time.sleep(1)
        try:
            tf_script = self.rules.build_tf_script(
                data, ru["config"], ru["rules"])
        except Exception as e:
            return(str(e))
        else:
            runner_id = self.evaluate_rule(tf_script)
            chk = False
            while not chk:
                c = self.get_result(runner_id)
                if c:
                    chk = True
                    return(c)

    def evaluate_rule(self, tf_script):
        watchdog_id = self.choose_watchdog()
        runner_uuid = "runner_"+str(uuid.uuid4())
        print("starting runner:", runner_uuid)
        print("watchdog id:",  watchdog_id)
        self.comms.publish(watchdog_id, json.dumps({
                           "sender": {"type": "master"}, "command": "start_runner", "data": {"uuid": runner_uuid, "script": tf_script}}))
        return runner_uuid

    def get_result(self, runner_id):
        result = {}
        sql = "SELECT * from runners where uuid='" + \
            runner_id+"' and state ='finished'"
        self.rules.mycursor.execute(sql)

        res = [dict((self.rules.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.rules.mycursor.fetchall()]
        if len(res) == 0:
            return False
        result["uuid"] = res[0]["uuid"]
        result["watchdog_uuid"] = res[0]["watchdog_uuid"]
        result["pid"] = res[0]["pid"]
        result["last_seen"] = res[0]["last_seen"]
        result["state"] = res[0]["state"]
        result["payload"] = json.loads(res[0]["payload"])

        return result

    def choose_watchdog(self):

        #results = []
        #rule_exit_code = 0
        sql = "SELECT watchdogs.uuid FROM  watchdogs LEFT OUTER JOIN runners ON watchdogs.uuid=runners.watchdog_uuid WHERE watchdogs.last_seen >=unix_timestamp()-4 ORDER BY COUNT( DISTINCT runners.uuid) DESC LIMIT 1"
        self.rules.mycursor.execute(sql)

        res = [dict((self.rules.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.rules.mycursor.fetchall()]
        if len(res) == 0:
            rule_exit_code = 1
        return res[0]["uuid"]

        #min_runners = 10000
        #min_runners_id = ""
        # for watchdog in self.watchdog_list:
        #    if len(watchdog["runners"]) < min_runners:
        #        min_runners = len(watchdog["runners"])
        #        min_runners_id = watchdog["id"]
        # return min_runners_id

    def parse_message(self, msg_obj):
        msg = Message(sender_id=msg_obj["sender"]["id"],
                      sendertype=msg_obj["sender"]["type"],
                      messagetype=msg_obj["messagetype"],
                      payload=msg_obj["payload"],
                      last_seen=time.time())
        return msg


class rules:

    def __init__(self):
        self.user = "andre"
        if config["Generic"]["db_engine"] == "MariaDB":
            self.mydb = mariadb.connect(
                host=config["DB"]["host"],
                user=config["DB"]["user"],
                password=config["DB"]["password"],
                database=config["DB"]["database"]
            )

        self.host = config["Redis"]["host"]
        self.port = int(config["Redis"]["port"])
        self.db = int(config["Redis"]["db"])
        self.mydb.autocommit = True
        self.mycursor = self.mydb.cursor()

    def check_rule_input(self, json_string):
        print(json_string)
        try:
            c = json.loads(json_string)
        except ValueError as err:
            raise ValueError("Invalid JSON")
        if isinstance(c, list):
            is_list = True
        else:
            raise ValueError("Not a valid JSON list")

    def build_tf_script(self, input_data, config_data, rule, template_file=config["Templates"]["default_template"], template_folder=config["Templates"]["default_folder"]):
        try:
            self.check_rule_input(json.dumps(input_data))
        except:
            raise
        environment = Environment(loader=FileSystemLoader(
            template_folder + "/"))
        template = environment.get_template(template_file)
        tf_script = template.render(
            input_data=json.dumps(input_data),
            config_data=config_data,
            rule=rule
        )
        return tf_script

    def read_excel(self, filename, sheetname="Sheet1"):
        try:
            sheetdata = pd.read_excel(
                filename, sheet_name=sheetname,  parse_dates=True).replace({np.nan: None})
            s2 = sheetdata.to_dict("records")
        except:
            return False
        else:
            return s2

    def write_excel(self, filename, sheetname, data):
        df = pd.DataFrame.from_dict(data)
        df.to_excel(filename, sheet_name=sheetname, index=False)
        return True

    def get_rule_list(self, is_valid=1, is_deleted=0):
        sql = "SELECT * FROM rules where is_valid={is_valid} and is_deleted={is_deleted}".format(
            is_valid=is_valid, is_deleted=is_deleted)
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]

        return res

    def read_rule(self, uuid):
        sql = "SELECT * FROM rules WHERE uuid='{0}'".format(uuid)
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]
        if len(res) == 0:
            raise ValueError('Rule UUID does not exist')
        return res[0]

    def set_rule_valid_flag(self, uuid, flag):
        sql = "UPDATE RULES SET is_valid=%s, last_modified_userid = %s WHERE uuid=%s"
        val = (flag, self.user, uuid)

        self.mycursor.execute(sql, val)
        self.mydb.commit()

    def save_rule(self, uid, customer_id, name, description, rules, data, is_valid, valid_from, valid_until, last_modified_userid, is_deleted):
        if uid == 0:
            uid = "rule_"+str(uuid.uuid4())

        # self.set_rule_valid_flag(uid, 0)
        sql = "INSERT INTO rules (uuid, customer_id, name, description, rules, config, is_valid, valid_from, valid_until, last_modified_userid, is_deleted) VALUES (%s, %s, %s,%s, %s,%s, %s, %s , %s,%s, %s)"
        val = (uid, customer_id, name, description,
               rules, json.dumps(data, default=str),  is_valid, valid_from, valid_until, last_modified_userid, is_deleted)
        self.mycursor.execute(sql, val)
        self.mydb.commit()
        return uid
