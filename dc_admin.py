import mariadb
import configparser
import codecs
import sys

config = configparser.ConfigParser()
config.read('config.ini')

error_codes = {
    0: "ok",

    500: "could not authenticate",
    501: "could not set password",
    502: "could not set permissions"
}


class admin:
    def __init__(self):
        if config["Generic"]["db_engine"] == "MariaDB":
            self.mydb = mariadb.connect(
                host=config["DB"]["host"],
                user=config["DB"]["user"],
                password=config["DB"]["password"],
                database=config["DB"]["database"]
            )
        self.mydb.autocommit = True
        self.mycursor = self.mydb.cursor()

    def event(self, object_type: str, object_uuid: str, event_type: str, user_uuid, success: bool = True, info: str = "", error_code=0):
        if success:
            success_bit = 1
        else:
            success_bit = 0
        sql = "INSERT INTO events (object_type, object_uuid, event_type, success, info, user_uuid, error_code) VALUES ('{object_type}', '{object_uuid}', '{event_type}', {success_bit}, '{info}', '{user_uuid}',{error_code})".format(
            object_type=object_type, object_uuid=object_uuid, event_type=event_type, success_bit=success_bit, info=info, user_uuid=user_uuid, error_code=error_code)

        self.mycursor.execute(sql)
        self.mydb.commit()

    def to_bytes(self, s):

        if type(s) is bytes:
            return s
        elif type(s) is str or (sys.version_info[0] < 3 and type(s) is unicode):
            return codecs.encode(s, 'utf-8')
        else:
            raise TypeError("Expected bytes or string, but got %s." % type(s))

    def api_reply(self, success=True, result_code=0, payload={}):
        return({"success": success, "result_code": result_code, "result_text": error_codes[result_code], "payload": payload})
