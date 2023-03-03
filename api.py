from fastapi import FastAPI, Query, Body
from typing import Union
import rules
import time
import json
from pydantic import BaseModel


r = rules.master()
rr = rules.rules()

app = FastAPI()
redis_server = r.comms
pubsub = redis_server.pubsub()
subscribe_key = r.receiving_topic
pubsub.psubscribe(**{subscribe_key: r.event_handler})

pubsub.run_in_thread(sleep_time=.01)


@app.get("/rules/")
async def get_rules(filter: Union[str, None] = None):
    return rr.get_rule_list()


@app.post("/rule/execute")
async def execute_rule(id: str = Query(title="UUID of the rule", description="UUID of the rule - must exist"), payload: dict = Body(...)):
    return(r.execute_rule(id=id, data=payload["data"]))


@app.get("/rule/test")
async def test_rule(id: str = Query(title="UUID of the rule", description="UUID of the rule - must exist")):
    return(r.test_rule(id))
