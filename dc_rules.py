import pandas as pd
import mariadb
import json
import configparser
import uuid
import redis
import time
from jinja2 import Environment, FileSystemLoader
import numpy as np
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


#class returndacta:
#    def __init(self):
#        pass#

#    def build_returndata(self, payload):
#        return {"time": time.time(), "payload": payload}





class master:
    def __init__(self):

        #self.rules = rules()
        self.host = config["Redis"]["host"]
        self.port = int(config["Redis"]["port"])
        self.db = int(config["Redis"]["db"])
        self.comms = redis.Redis(host=self.host, port=self.port,
                                 db=self.db, decode_responses=True)

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
        self.mycursor.execute('TRUNCATE TABLE watchdogs')
        self.mydb.commit()

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

        #sql = "DELETE FROM runners WHERE watchdog_uuid='"+id+"'"

        # self.mycursor.execute(sql)
        # self.mydb.commit()

        for runner in runners:
            print(runner)
            sql = "REPLACE INTO runners (uuid, watchdog_uuid, pid, last_seen, state, payload, last_modified_user_uuid) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            val = (runner[0], id, runner[1],
                   runner[2], runner[3], json.dumps(runner[4]), self.user)
            self.mycursor.execute(sql, val)
            self.mydb.commit()



    def choose_watchdog(self):
        watchdogs = 0
        while watchdogs == 0:
            res = self.get_watchdogs()

            watchdogs = len(res)

        return res[0]["uuid"]

   

    def get_watchdogs(self):
        sql = "SELECT watchdogs.uuid FROM  watchdogs LEFT OUTER JOIN runners ON watchdogs.uuid=runners.watchdog_uuid WHERE watchdogs.last_seen >=unix_timestamp()-4 ORDER BY COUNT( DISTINCT runners.uuid) DESC LIMIT 1"
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]
        return res

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

        self.comms = redis.Redis(host=config["Redis"]["host"], port=int(config["Redis"]["port"]),
                                 db=int(config["Redis"]["db"]), decode_responses=True)
        self.mydb.autocommit = True
        self.mycursor = self.mydb.cursor()
        self.master = master()


    
    def test_rule(self, id):
        results = []
        rule_exit_code = 0
        sql = "SELECT * FROM rule_tests where is_valid=1 and is_deleted=0 and rule_uuid=%(id)s"
        self.mycursor.execute(sql, {'id':id})

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]
        if len(res) == 0:
            rule_exit_code = 1
        for test in res:
            expected_output = json.loads(test["expected_output"])
            input_data = json.loads(test["input_data"])
            res2 = self.execute_rule(id, input_data)
            print("-----input:----", test["input_data"])
            print("-----result:-----", res2)
            result = res2["matches"][0]["match"]
            results.append({"input": input_data, "expected_result": expected_output,
                           "actual_result": result, "passed": result == expected_output})

        res = {"rule_exit_code": rule_exit_code, "results": results}
        return res

    def execute(self, rule_uuid, data):
        try:
            ru = self.read(rule_uuid)
        except Exception as e:
            return(str(e))
        #time.sleep(1)
        try:
            tf_script = self.build_tf_script(
                data, ru["config"], ru["rules"])
        except Exception as e:
            return(str(e))
        else:
            runner_id = self.evaluate(tf_script, rule_uuid)
            chk = False
            while not chk:
                c = self.get_result(runner_id)
                if c:
                    chk = True

                    return(json.loads(c["payload"]))

    def evaluate(self, tf_script, rule_uuid):
        watchdog_id = self.master.choose_watchdog()
        runner_uuid = str(uuid.uuid4())
        print("starting runner:", runner_uuid)
        print("watchdog id:",  watchdog_id)
        self.comms.publish(watchdog_id, json.dumps({
                           "sender": {"type": "master"}, "command": "start_runner", "data": {"rule_uuid": rule_uuid, "uuid": runner_uuid, "script": tf_script}}))
        return runner_uuid

    def get_result(self, runner_id):
        result = {}
        sql = "SELECT * from runners where uuid='" + \
            runner_id+"' and state ='finished'"
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]
        if len(res) == 0:
            return False
        result["uuid"] = res[0]["uuid"]
        result["watchdog_uuid"] = res[0]["watchdog_uuid"]
        result["pid"] = res[0]["pid"]
        result["last_seen"] = res[0]["last_seen"]
        result["state"] = res[0]["state"]
        result["payload"] = json.loads(res[0]["payload"])

        return result

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

    def build_tf_script(self, input_data, config_data, rule, template_file=config["Templates"]["default_template"]):
        try:
            self.check_rule_input(json.dumps(input_data))
        except:
            raise
        environment = Environment(loader=FileSystemLoader(
            config["Templates"]["default_folder"] + "/"))
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

    def get_list(self, is_valid=1, is_deleted=0):
        sql = """
            SELECT 
	            r.uuid, 
	            r.customer_uuid,
	            r.name AS rule_name,
	            r.description,
	            r.rules,
	            c.config,
	            c.name AS config_name,
	            r.config_uuid,
	            r.is_valid,
	            r.is_deleted,
	            r.last_modified_datetime,
	            r.last_modified_user_uuid
            FROM
	            rules r LEFT OUTER JOIN
	            configs c ON r.config_uuid=c.uuid
            WHERE
                r.is_valid = %(is_valid)i AND
                r.is_deleted = %(is_deleted)i AND
	            r.is_valid=1 AND 
	            r.is_deleted=0 AND
    	        NOW() BETWEEN r.valid_from AND r.valid_until AND
	            if(c.uuid IS NOT NULL, 
		            c.is_current=1 AND
		            c.is_deleted=0 AND
		            NOW() BETWEEN c.valid_from AND c.valid_until
                )
                
        """
        self.mycursor.execute(sql, {'is_deleted':is_deleted, 'is_valid':is_valid})

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]

        return res

    def read(self, uuid):
        sql = """
            SELECT 
	            r.uuid, 
	            r.customer_uuid,
	            r.name AS rule_name,
	            r.description,
	            r.rules,
	            c.config,
	            c.name AS config_name,
	            r.config_uuid,
	            r.is_valid,
	            r.is_deleted,
	            r.last_modified_datetime,
	            r.last_modified_user_uuid
            FROM
	            rules r LEFT OUTER JOIN
	            configs c ON r.config_uuid=c.uuid
            WHERE
	            r.is_valid=1 AND 
	            r.is_deleted=0 AND
    	        NOW() BETWEEN r.valid_from AND r.valid_until AND
	            if(c.uuid IS NOT NULL, 
		            c.is_current=1 AND
		            c.is_deleted=0 AND
		            NOW() BETWEEN c.valid_from AND c.valid_until, 1=1
                )
                AND r.uuid=%(uuid)s
        """
        #sql = "SELECT * FROM rules WHERE uuid='{0}'".format(uuid)
        self.mycursor.execute(sql, {'uuid': uuid})

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]
        if len(res) == 0:
            raise ValueError('Rule UUID does not exist')
        return res[0]

    def set_valid_flag(self, uuid, flag):
        sql = """ 
            UPDATE 
                RULES 
            SET 
                is_valid=%(is_valid)i, 
                last_modified_userid = %(last_modified_userid)s 
            WHERE 
                uuid=%(uuid)s"""
        
        self.mycursor.execute(sql, {'is_valid':flag, 'last_modified_userid':self.user, 'uuid':uuid})
        self.mydb.commit()

    def save(self, uid, customer_id, name, description, rules, data, is_valid, valid_from, valid_until, last_modified_userid, is_deleted):
        if uid == 0:
            uid = "rule_"+str(uuid.uuid4())

        # self.set_rule_valid_flag(uid, 0)
        sql = """
            INSERT INTO rules (
                uuid, 
                customer_id, 
                name, 
                description, 
                rules, config, 
                is_valid, 
                valid_from, 
                valid_until, 
                last_modified_userid, 
                is_deleted
            ) 
            VALUES (
                    %s, 
                    %s, 
                    %s,
                    %s,
                    %s,
                    %s, 
                    %s, 
                    %s, 
                    %s,
                    %s, 
                    %s)
            """
        val = (uid, customer_id, name, description,
               rules, json.dumps(data, default=str),  is_valid, valid_from, valid_until, last_modified_userid, is_deleted)
        self.mycursor.execute(sql, val)
        self.mydb.commit()
        return uid
