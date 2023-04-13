from fastapi import FastAPI, Query, Body, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Union
import rules
import time
import json
from pydantic import BaseModel

sys = rules.system()
r = rules.master()
rr = rules.rules()
user = rules.user()

app = FastAPI()
redis_server = r.comms
pubsub = redis_server.pubsub()
subscribe_key = r.receiving_topic
pubsub.psubscribe(**{subscribe_key: r.event_handler})
pubsub.run_in_thread(sleep_time=.01)

security = HTTPBasic()


@app.get("/sys/selftest")
async def selftest():
    return sys.selftest()


@app.get("/auth/")
async def read_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    return user.authenticate(credentials.username, credentials.password)


@app.get("/rules/")
async def get_rules():
    return rr.get_rule_list()


@app.post("/rule/execute")
async def execute_rule(id: str = Query(title="UUID of the rule", description="UUID of the rule - must exist"), payload: dict = Body(...)):
    return(r.execute_rule(id=id, data=payload["data"]))


@app.get("/rule/test")
async def test_rule(id: str = Query(title="UUID of the rule", description="UUID of the rule - must exist")):
    return(r.test_rule(id))
