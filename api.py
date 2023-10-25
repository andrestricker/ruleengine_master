from fastapi import FastAPI, Query, Body, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Union
import dc_rules
import dc_configs
import dc_users
import dc_admin
from dateutil.parser import parse
import time
import json
from pydantic import BaseModel

a = dc_admin.admin()
sys = dc_rules.system()
r = dc_rules.master()
rr = dc_rules.rules()
c = dc_configs.configs()
user = dc_users.user()

app = FastAPI()
redis_server = r.comms
pubsub = redis_server.pubsub()
redis_server.flushdb()
pubsub.psubscribe(**{r.receiving_topic: r.event_handler})
pubsub.run_in_thread(sleep_time=.01)

security = HTTPBasic()


# @app.get("/configs/")
# async def get_configs():
#    return c.get_config_list(is_current=0)


@app.get("/config/get/")
async def get_config(uuid: Union[str, None] = None, show_current: bool = True, show_deleted: bool = False, valid_at: Union[str, None] = None):
    return(c.get_config(uuid=uuid, show_deleted=show_deleted, show_current=show_current, valid_at=valid_at))


@app.post("/config/write/{id}")
async def write_config(id: str = None, payload: dict = Body(...)):
    this_conf = dc_configs.config_item
    this_conf.uuid = id
    this_conf.customer_uuid = payload["customer_uuid"]
    this_conf.name = payload["name"]
    this_conf.config = payload["config"]
    if "is_current" in payload:
        this_conf.is_current = payload["is_current"]

    if "is_deleted" in payload:
        this_conf.is_deleted = payload["is_deleted"]

    if "valid_from" in payload:
        this_conf.valid_from = parse(payload["valid_from"])

    if "valid_until" in payload:
        this_conf.valid_until = parse(payload["valid_until"])

    this_conf.last_modified_user_uuid = c.user_uuid

    this_conf.description = payload["description"]

    return c.write_config(this_conf)


@app.get("/rules/")
async def get_rules():
    return rr.get_rule_list()


@app.post("/rule/execute")
async def execute_rule(id: str = Query(title="UUID of the rule", description="UUID  of the rule - must exist"), payload: dict = Body(...)):
    return(r.execute_rule(rule_uuid=id, data=payload["data"]))


@app.get("/rule/test")
async def test_rule(id: str = Query(title="UUID of the rule", description="UUID of the rule - must exist")):
    return(r.test_rule(id))


@app.post("/user/auth")
async def auth_user(payload: dict = Body(...)):
    return user.authenticate(payload["username"], payload["customer_uuid"], payload["password"])


@app.post("/user/set_password")
async def set_password(payload: dict = Body(...)):
    res = user.set_password(payload["uuid"], payload["password"])

    if res == True:
        return a.api_reply(True)
    else:
        return a.api_reply(False, 501)


@app.post("/user/set_permissions")
async def set_permissions(payload: dict = Body(...)):
    res = user.set_permissions(payload["uuid"], payload["permissions"])
    if res == True:
        return a.api_reply(True)
    else:
        return a.api_reply(False, 502)


@app.get("/sys/selftest")
async def selftest():
    return sys.selftest()
