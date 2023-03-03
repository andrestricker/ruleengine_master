import pandas as pd
import mariadb
import json
import configparser
import uuid
import redis
import time
import subprocess
import os
import signal
from jinja2 import Environment, FileSystemLoader
import numpy as np
import logging
from dataclasses import dataclass

config = configparser.ConfigParser()
config.read('config.ini')


@dataclass
class Message:
    sender_id: str
    messagetype: str
    sendertype: str
    payload: dict
    last_seen: float


class master:
    def __init__(self):
        self.rules = rules()
        self.comms = redis.Redis(host=self.rules.host, port=self.rules.port,
                                 db=self.rules.db, decode_responses=True)

        self.receiving_topic = "to_master_from_watchdog"
        self.watchdog_list = []

    def event_handler(self, raw_message):

        msg_obj = json.loads(raw_message["data"])
        msg = self.parse_message(msg_obj)

        if msg.sendertype == "watchdog":
            self.set_watchdog_state(
                msg.sender_id, msg.payload["runners"], msg.last_seen)

    def set_watchdog_state(self, id, runners, last_seen):
        try:
            c = self.get_watchdog_index(id)
        except ValueError:
            self.watchdog_list.append(
                {"id": id, "runners": runners, "last_seen": last_seen})
        else:
            self.watchdog_list[c]["id"] = id
            self.watchdog_list[c]["runners"] = runners

    def get_watchdog_index(self, id):
        for c, watchdog in enumerate(self.watchdog_list):
            if watchdog["id"] == id:
                return c
        raise ValueError("Watchdog not found")

    def test_rule(self, id):
        results = []
        sql = "SELECT * FROM rule_tests where is_valid=1 and is_deleted=0"
        self.rules.mycursor.execute(sql)

        res = [dict((self.rules.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.rules.mycursor.fetchall()]

        for test in res:
            print(test["input_data"])
            expected_output = json.loads(test["expected_output"])
            input_data = json.loads(test["input_data"])
            res2 = self.execute_rule(id, input_data)

            result = res2["payload"]["rule_result"][0]["result"]
            results.append({"input": input_data, "expected_result": expected_output,
                           "actual_result": result, "passed": result == expected_output})

        return results

    def execute_rule(self, id, data):
        ru = self.rules.read_rule(id)
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
        for watchdog in self.watchdog_list:
            for runner in watchdog["runners"]:
                if runner["state"] == "finished" and runner["payload"]["rule_exit_code"] == 0 and runner["id"] == runner_id:
                    return runner  # ["payload"]["rule_result"]
        return False

    def choose_watchdog(self):
        min_runners = 10000
        min_runners_id = ""
        for watchdog in self.watchdog_list:
            if len(watchdog["runners"]) < min_runners:
                min_runners = len(watchdog["runners"])
                min_runners_id = watchdog["id"]
        return min_runners_id

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
