import rules


r = rules.master()
rr = rules.rules()

redis_server = r.comms
pubsub = redis_server.pubsub()
subscribe_key = r.receiving_topic
pubsub.psubscribe(**{subscribe_key: r.event_handler})

pubsub.run_in_thread(sleep_time=.01)


ru = rr.read_rule("9b7749b0-98be-4a0e-9858-945442e5b32d")

#print(ru[0]["config"])

#print(ru[0]["rules"])

try: 
    tf_script = rr.build_tf_script('[{"tour": "A"}, {"tour": "DMS"},{"tour": "GFM"}]', ru[0]["config"], ru[0]["rules"])
except Exception as e:
    print(str(e))
else:     
    r.evaluate_rule(tf_script)

#print(tf_script)

