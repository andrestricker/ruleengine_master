import mariadb
import dc_rules
import redis
import configparser
import platform
import sysconfig
import psutil
import os


config = configparser.ConfigParser()
config.read('config.ini')


class system:
    def __init__(self):
        self.rules = dc_rules.rules()
        self.comms = redis.Redis(host=config["Redis"]["host"], port=int(config["Redis"]["port"]),
                                 db=int(config["Redis"]["db"]), decode_responses=True)

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
            sql = "SELECT * FROM runners WHERE watchdog_uuid='"+watchdog["uuid"]+"'"
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
