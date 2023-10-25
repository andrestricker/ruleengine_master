import mariadb
import configparser
import bcrypt
import json
from dc_admin import admin
import uuid

config = configparser.ConfigParser()
config.read('config.ini')

a = admin()


class user:
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

        self.user_uuid = ""
        self.token = ""
        self.expires = 0

    def set_password(self, user_uuid, password):
        password_hash = self.hash_password(password)

        self.mycursor.callproc('p_user_set_password', [
                               user_uuid, password_hash])
        a.event("user", user_uuid, "set_password", self.user_uuid)

        return True

    def set_permissions(self, user_uuid, permissions):
        permissions_json = json.dumps(permissions)

        self.mycursor.callproc('p_user_set_permissions', [
                               user_uuid, permissions_json])
        return True

    def hash_password(self, password):
        # converting password to array of bytes
        bytes = password.encode('utf-8')

        # generating the salt
        salt = bcrypt.gensalt()

        # Hashing the password
        hash = bcrypt.hashpw(bytes, salt)
        return hash

    def check_password(self, entered_password, db_password_hash):

        # encoding user password
        entered_password_encoded = a.to_bytes(entered_password)
        db_password_hash_encoded = a.to_bytes(db_password_hash)

        result = bcrypt.checkpw(entered_password_encoded,
                                db_password_hash_encoded)

        return result

    def authenticate(self, username, customer_uuid, password):
        sql = "SELECT password_hash, uuid FROM users where username='{username}' AND customer_uuid='{customer_uuid}' AND is_deleted=0 and is_valid=1 and NOW() BETWEEN valid_from AND valid_until ".format(
            username=username, customer_uuid=customer_uuid)
        self.mycursor.execute(sql)

        for row in self.mycursor.fetchall():
            db_password_hash = row[0]
            user_uuid = row[1]

        if self.check_password(a.to_bytes(
                password), a.to_bytes(db_password_hash)):

            token, valid_until = self.refresh_session(user_uuid=user_uuid)

            return_payload = {
                "token": token,
                "expires": valid_until}
            a.event(object_type="user",
                    object_uuid=user_uuid, info="Username: "+username, event_type="login", user_uuid=self.user_uuid)

            return(a.api_reply(True, 0, return_payload))
        else:
            a.event(object_type="user", success=False,
                    object_uuid=user_uuid, info="Username: "+username, error_code=500, event_type="login", user_uuid=self.user_uuid)
            return(a.api_reply(False, 500, {}))

    def check_session(self, session_uuid):
        pass

    def refresh_session(self, user_uuid):
        sql = "SELECT uuid FROM sessions WHERE user_uuid='{user_uuid}' AND valid_until >= NOW()".format(
            user_uuid=user_uuid)
        self.mycursor.execute(sql)

        if self.mycursor.rowcount == 0:
            session_uuid = None
        else:
            rows = self.mycursor.fetchall()
            for row in rows:
                session_uuid = row[0]

        if session_uuid == None:
            session_uuid = uuid.uuid4()

        expiry_seconds = int(config["Sessions"]["token_expiry_seconds"])
        sql = "REPLACE INTO sessions (uuid, user_uuid, valid_until) VALUES ('{session_uuid}', '{user_uuid}', NOW() + INTERVAL {expiry_seconds} SECOND)".format(
            session_uuid=session_uuid, user_uuid=user_uuid, expiry_seconds=expiry_seconds)

        self.mycursor.execute(sql)
        self.mydb.commit()
        self.user_uuid = user_uuid
        return self.get_token_data(session_uuid)

    def get_token_data(self, session_uuid):
        sql = "SELECT uuid, valid_until FROM sessions WHERE uuid='{session_uuid}' AND valid_until >=NOW()".format(
            session_uuid=session_uuid)
        self.mycursor.execute(sql)
        if self.mycursor.rowcount == 0:
            session_uuid = None
        else:
            rows = self.mycursor.fetchall()
            for row in rows:
                session_uuid = row[0]
                valid_until = row[1]
        return session_uuid, valid_until

    def build_token(self):
        return str(uuid.uuid4())

    def is_in_group(self, groupname):

        pass
