import rules


r = rules.master()
rr = rules.rules()

redis_server = r.comms
pubsub = redis_server.pubsub()
subscribe_key = r.receiving_topic
pubsub.psubscribe(**{subscribe_key: r.event_handler})

pubsub.run_in_thread(sleep_time=.01)


ru = rr.read_rule("9b7749b0-98be-4a0e-9858-945442e5b32d")

# print(ru[0]["rules"])
# print(ru[0]["config"])

tf_script = rr.build_tf_script({"tour": "A"}, ru[0]["config"], ru[0]["rules"])
# print(tf_script)

r.evaluate_rule(tf_script)
