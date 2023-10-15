import mariadb
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


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

    def get_config_list(self, is_current=1, is_deleted=0):
        sql = """
            SELECT 
	            c.uuid, 
	            c.customer_uuid,
	            c.name AS config_name,
	            c.description,
	            c.config,
	            c.is_current,
	            c.is_deleted,
                c.valid_from, 
                c.valid_until,
	            c.last_modified_datetime,
	            c.last_modified_user_uuid
            FROM
	            configs c
            WHERE
                c.is_current = {is_current} AND
                c.is_deleted = {is_deleted} AND
	            NOW() BETWEEN c.valid_from AND c.valid_until
                
        """.format(
            is_current=is_current, is_deleted=is_deleted)
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]

        return res

    def get_config(self, uuid, show_deleted, show_current, valid_at):
        deleted_str = "0"
        show_current_str = "1,0"
        valid_at_str = "NOW()"
        if show_deleted == True:
            deleted_str = "1,0"

        if show_current == True:
            show_current_str = "1"

        if valid_at:
            valid_at_str = "'"+valid_at+"'"

        sql = """
            SELECT 
	            c.uuid, 
	            c.customer_uuid,
	            c.name AS config_name,
	            c.description,
	            c.config,
	            c.is_current,
	            c.is_deleted,
                c.valid_from, 
                c.valid_until,
	            c.last_modified_datetime,
	            c.last_modified_user_uuid
            FROM
	            configs c
            WHERE
                c.is_current IN ({is_current}) AND
                c.is_deleted IN ({is_deleted}) AND
	            {valid_at} BETWEEN c.valid_from AND c.valid_until AND
                c.uuid='{uuid}'
                
        """.format(
            is_current=show_current_str, is_deleted=deleted_str, valid_at=valid_at_str, uuid=uuid)

        print(sql)
        self.mycursor.execute(sql)

        res = [dict((self.mycursor.description[i][0], value)
                    for i, value in enumerate(row)) for row in self.mycursor.fetchall()]

        return res

        pass
