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


class watchdog:
    def __init__(self):

        self.rules = rules()
        self.comms = redis.Redis(host=self.rules.host, port=self.rules.port,
                                 db=self.rules.db, decode_responses=True)

        self.id = "watchdog-"+str(uuid.uuid4())
        self.sending_topic = "to_runner_from_watchdog"
        self.sending_topic_master = "to_master_from_watchdog"

        self.receiving_topic = "to_watchdog_from_*"
        self.subject_list = []

    def register(self):
        message = self.rules.build_message(self.id,
                                           "watchdog", "info", {"state": "register"})

        self.comms.publish(self.sending_topic_master, message)

    def event_handler(self, raw_message):
        print(raw_message)
        topic = raw_message["channel"]
        message = json.loads(raw_message["data"])

        if topic == "to_watchdog_from_runner":
            self.event_handler_runner(message)

        elif topic == "to_watchdog_from_master":
            self.event_handler_master(message)

    def event_handler_runner(self, message):
        pid = message["sender"]["pid"]
        timestamp = message["timestamp"]
        state = message["payload"]["state"]
        self.set_runner_state(pid, timestamp, state)

    def event_handler_master(self, message):
        command = message["command"]
        if command == "start_runner":
            tf_script = message["script"]
            full_filename = self.build_tf_script_file(tf_script)
            runner_id = "runner_"+str(uuid.uuid4())
            self.start_runner(full_filename, runner_id)

    def set_runner_state(self, pid, timestamp, state, id=""):
        process_index = self.get_subject_index(pid)

        if process_index is False:
            self.subject_list.append(
                {"id": id, "pid": pid, "last_seen": timestamp, "state": state})
        else:
            self.subject_list[process_index]["id"] = id
            self.subject_list[process_index]["last_seen"] = timestamp
            self.subject_list[process_index]["state"] = state

    def get_expired_runners(self, timeout=int(config["Runner"]["timeout"])):
        expired_runners = []
        for subject in self.subject_list:
            if subject["last_seen"]+timeout <= time.time():
                expired_runners.append(subject["pid"])
        return expired_runners

    def start_runner(self, filename, id=""):
        proc = subprocess.Popen(['python', './runner.py', filename], id)
        pid = proc.pid
        self.set_runner_state(pid, time.time(), "started", id)
        return proc.pid

    def kill_runner(self, pid):
        process_index = self.get_subject_index(pid)

        if os.name == 'nt':
            os.system("taskkill /F /PID "+str(pid))
        else:
            os.kill(pid, signal.SIGTERM)

        del(self.subject_list[process_index])

    def build_tf_filename(self):
        id = "file_"+str(uuid.uuid4())

        full_filename = config["Templates"]["temp_folder"] + \
            "/"+id+".tf"
        return full_filename

    def build_tf_script_file(self, tf_script):
        full_filename = self.build_tf_filename()
        f = open(full_filename, "w")
        f.write(tf_script)
        return full_filename

    def get_subject_index(self, pid):
        c = 0
        for subject in self.subject_list:
            if subject["pid"] == pid:
                return c
            c = c+1
        return False


class runner:
    def __init__(self, id):
        self.rules = rules()
        self.state = "idle"
        self.id = id
        self.comms = redis.Redis(host=self.rules.host, port=self.rules.port,
                                 db=self.rules.db, decode_responses=True)

        self.sending_topic = "to_watchdog_from_runner"
        self.receiving_topic = "to_runner_from_watchdog"

    def start(self, filename):
        proc = subprocess.Popen(['python', './runner.py', filename])
        return proc.pid

    def set_result(self, result):
        res = ''.join(result.splitlines())
        message = self.rules.build_message(self.id,
                                           "runner", "info", {"state": "state", "result": res})
        self.comms.publish(self.sending_topic, message)

    def set_state(self, state):
        self.state = state
        message = self.rules.build_message(self.id,
                                           "runner", "info", {"state": state})
        self.comms.publish(self.sending_topic, message)

    def execute_tf_script(self, tf_scriptfile):
        proc = subprocess.run(config["Internal"]["ts_installation_folder"]+"/"+config["Internal"]
                              ["tf_executable"]+" "+config["Internal"]["tf_parameters"]+" "+tf_scriptfile, capture_output=True,  text=True)
        return proc


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

    def build_tf_script(self, input_data, config_data, rule, template_file=config["Templates"]["default_template"], template_folder=config["Templates"]["default_folder"]):

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
                filename, sheet_name=sheetname,  parse_dates=True)
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
