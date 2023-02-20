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

config = configparser.ConfigParser()
config.read('config.ini')


class master:
    def __init__(self):
        self.rules = rules()
        self.comms = redis.Redis(host=self.rules.host, port=self.rules.port,
                                 db=self.rules.db, decode_responses=True)

        self.sending_topic = "to_watchdog_from_master"
        self.receiving_topic = "to_master_from_watchdog"
        self.watchdog_list = []

    # def launch_runner(self, uuid):

    def event_handler(self, raw_message):
        print(json.loads(raw_message["data"]))

    def evaluate_rule(self, tf_script):
        runner_uuid = "runner_"+str(uuid.uuid4())
        self.comms.publish(self.sending_topic, json.dumps({
                           "command": "start_runner", "data": {"uuid": runner_uuid, "script": tf_script}}))


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

        self.mycursor = self.mydb.cursor()

    def check_rule_input(self, json_string):
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
            self.check_rule_input(input_data)
        except:
            raise
        environment = Environment(loader=FileSystemLoader(
            template_folder + "/"))
        template = environment.get_template(template_file)
        tf_script = template.render(
            input_data=input_data,
            config_data=config_data,
            rule=rule
        )
        return tf_script

    def build_message(self, id, sender_type="runner", message_type="info", payload={}):
        sender_pid = os.getpid()
        return json.dumps({"timestamp": time.time(), "sender": {"type": sender_type, "pid": sender_pid}, "messagetype": message_type, "payload": payload})

    def read_excel(self, filename, sheetname="Sheet1"):
        try:
            sheetdata = pd.read_excel(
                filename, sheet_name=sheetname,  parse_dates=True).replace({np.nan: None})
            s2 = sheetdata.to_dict("records")
            # print(s2)
        except:
            return False
        else:
            return s2

    def write_excel(self, filename, sheetname, data):
        df = pd.DataFrame.from_dict(data)
        df.to_excel(filename, sheet_name=sheetname, index=False)
        return True

    def read_rule(self, uuid):
        sql = "SELECT * FROM rules WHERE uuid='{0}'".format(uuid)
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]

        return res

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
