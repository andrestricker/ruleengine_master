import mariadb
from datetime import datetime, timedelta
from dateutil.parser import parse
import configparser
from dataclasses import dataclass, asdict
from uuid import uuid4


config = configparser.ConfigParser()
config.read('config.ini')


@dataclass
class config_item:
    uuid: str = ""
    customer_uuid: str = ""
    name: str = ""
    description: str = ""
    config: dict = ""
    last_modified_user_uuid: str = ""
    is_current: bool = True
    is_deleted: bool = False
    valid_from: str = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    valid_until: str = '2099-12-31 23:59:59'


class configs:
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

        self.user_uuid = "bc1b3751-8ea2-4041-b5a4-980bc216f1ec"
        self.customer_uuid = "fda43a80-1b65-4190-bc6a-f86271c3344b"

    def get_config_list(self, is_current=1, is_deleted=0):
        sql = """
            SELECT 
	            c.uuid, 
	            c.customer_uuid,
	            c.name,
	            c.description,
	            c.config,
	            if(c.is_current = true, 1, 0) as is_current,
	            if(c.is_deleted = true, 1, 0) as is_deleted,
                CAST(c.valid_from as CHAR) as valid_from , 
                CAST(c.valid_until as CHAR) as valid_until, 
	            CAST(c.last_modified_datetime as CHAR) as last_modified_datetime,
	            c.last_modified_user_uuid
            FROM
	            configs c
            WHERE
                c.is_current = {is_current} AND
                c.is_deleted = {is_deleted} 
                
        """.format(
            is_current=is_current, is_deleted=is_deleted)
        print(sql)
        self.mycursor.execute(sql)

        config_list = []
        for row in self.mycursor.fetchall():
            c = config_item()
            for i, value in enumerate(row):

                setattr(c, self.mycursor.description[i][0], value)

            print(asdict(c))
            config_list.append(c)

        return config_list

    def get_config(self,  show_deleted, show_current, valid_at, uuid):

        deleted_str = "0"
        show_current_str = "1,0"
        valid_at_str = "1 = 1"
        uuid_str = "1 = 1"

        if uuid:
            uuid_str = "UUID='"+uuid+"'"

        if show_deleted == True:
            deleted_str = "1,0"

        if show_current == True:
            show_current_str = "1"

        if valid_at:
            valid_at_str = "'"+valid_at+"' BETWEEN c.valid_from AND c.valid_until"

        sql = """
            SELECT 
	            c.uuid, 
	            c.customer_uuid,
	            c.name,
	            c.description,
	            c.config,
	            CAST(c.is_current as INT) as is_current,
	            CAST(c.is_deleted as INT) as is_deleted,
                CAST(c.valid_from as CHAR) as valid_from, 
                CAST(c.valid_until as CHAR) as valid_until,
	            cast(c.last_modified_datetime as CHAR) as last_modified_datetime,
	            c.last_modified_user_uuid
            FROM
	            configs c
            WHERE
                c.is_current IN ({is_current}) AND
                c.is_deleted IN ({is_deleted}) AND
	            {valid_at}  AND
                {uuid_str}
                
        """.format(
            is_current=show_current_str, is_deleted=deleted_str, valid_at=valid_at_str, uuid_str=uuid_str)

        print(sql)
        self.mycursor.execute(sql)

        config_list = []
        for row in self.mycursor.fetchall():
            c = config_item()
            for i, value in enumerate(row):

                setattr(c, self.mycursor.description[i][0], value)

            print(asdict(c))
            config_list.append(c)

        return config_list

        # res = [dict((self.mycursor.description[i][0], value)
        #            for i, value in enumerate(row)) for row in self.mycursor.fetchall()]

        # return res

    def write_config(self, config: config_item):

        sql = """
            UPDATE configs SET is_current=false, valid_until=NOW()-INTERVAL 1 SECOND WHERE is_current=true AND uuid='{uuid}'
        """.format(uuid=config.uuid)
        print(sql)
        self.mycursor.execute(sql)

        sql = """
            INSERT INTO configs 
            (uuid, customer_uuid, name, description, config, is_current, is_deleted, valid_from, valid_until, last_modified_user_uuid)
            VALUES
            ('{uuid}', '{customer_uuid}', '{name}', '{description}', '{config}', {is_current}, {is_deleted}, NOW(), '2099-12-31',  '{last_modified_user_uuid}') 
        """.format(uuid=config.uuid, customer_uuid=config.customer_uuid, name=config.name, description=config.description, config=config.config, is_deleted=config.is_deleted, is_current=config.is_current, valid_from=config.valid_from.strftime("%Y-%m-%d %H:%M:%S"), valid_until=config.valid_until.strftime("%Y-%m-%d %H:%M:%S"), last_modified_user_uuid=self.user_uuid)

        print(sql)
        self.mycursor.execute(sql)
