from fastapi import FastAPI, Query, Body, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Union
import dc_rules
import dc_configs
import time
import json
from pydantic import BaseModel

sys = dc_rules.system()
r = dc_rules.master()
rr = dc_rules.rules()
c = dc_configs.configs()
user = dc_rules.user()

app = FastAPI()
redis_server = r.comms
pubsub = redis_server.pubsub()
redis_server.flushdb()
pubsub.psubscribe(**{r.receiving_topic: r.event_handler})
pubsub.run_in_thread(sleep_time=.01)

security = HTTPBasic()


@app.get("/configs/")
async def get_configs():
    return c.get_config_list()


@app.get("/config/get/{id}")
async def get_config(id: str, show_current: bool = True, show_deleted: bool = False, valid_at: Union[str, None] = None):
    return(c.get_config(uuid=id, show_deleted=show_deleted, show_current=show_current, valid_at=valid_at))


@app.get("/rules/")
async def get_rules():
    return rr.get_rule_list()


@app.post("/rule/execute")
async def execute_rule(id: str = Query(title="UUID of the rule", description="UUID  of the rule - must exist"), payload: dict = Body(...)):
    return(r.execute_rule(rule_uuid=id, data=payload["data"]))


@app.get("/rule/test")
async def test_rule(id: str = Query(title="UUID of the rule", description="UUID of the rule - must exist")):
    return(r.test_rule(id))


@app.get("/sys/selftest")
async def selftest():
    return sys.selftest()


@app.get("/auth/")
async def read_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    return user.authenticate(credentials.username, credentials.password)
