
## State PubSub and ClientNotify Queue

### state updates

    {
        "cmd" : "state",
        "arg" : # list of pilot or unit dicts
                # minimal content:
                [{
                  "type" : "pilot | unit",
                  "uid"  : "thing.0000",
                  "state": "NEW"
                }]
    }


## Agent Unschedule PubSub

### unit unschedule

Notify the Agent scheduler about resources being freed up

    {
        "cmd" : "unschedule",
        "arg" : # list of unit dicts
                # minimal content:
                [{
                  "uid"   : "unit.0000",   # ID if unit to unschedule
                  "slots" : []             # resources to be freed
                }]
    }


## Control Pubsub

### alive messages

    {
        "cmd" : "alive",
        "arg" : {
                  "uid"   : "pilot.0000",  # what is alive
                  "owner" : "pmgr.0000",   # who owns that thing optional
                  "src"   : "agent"        # where does the message originate
                }
    }


### heartbeat messages

    {
        "cmd" : "heartbeat",
        "arg" : {
                  "uid"  : "pilot.0000",  # what is alive
                  "time" : 12345.6788     # EPOCH when heartbeat was issued
                }
    }


### component or module termination

    {
        "cmd" : "stop",
        "arg" : # optional, either one of the options below:
                [
                    null,                   # stop all entities
                    {"type": "pilot"},      # stop all of this type
                    {"uid" : "pilot.0000"}  # stop the one with this ID
                ]
    }


### unit or pilot cancelation

    {
        "cmd" : "cancel",
        "arg" : # optional, either one of the options below:
                [
                    null,                     # cancel all entities
                    {"type": "pilot"},        # cancel all of this type
                    {"uid" : ["pilot.0000"]}  # cancel the one(s) with these IDs
                ]
    }


### pilot staging requests

pmgr asking the pmgr.launcher to stage data for a pilot

    {
        "cmd" : "staging_request",
        "arg" : {
                    "uid"  : "pmgr.0000",      # who sends the request
                    "recv" : "launcher.0000",  # who should enact it
                    "sds"  : []                # list of staging directives
                }
    }


### pilot staging results

.launcher reports completion / failinng of staging to pmgr

    {
        "cmd" : "staging_result",
        "arg" : {
                    "uid"    : "launcher.0000",   # who sends the result
                    "recv"   : "pmgr.0000",       # who should read it
                    "result" : ["DONE", "FAILED"] # one of there
                    "sds"    : []                 # list of staging directives
                }
    }




### add pilots

Umgr informs Umgr.Scheduler about new pilots

    {
        "cmd" : "add_pilots",
        "arg" : {
                     "pilots": [],          # list of pilot dicts
                     "umgr"  : "umgr.0000"  # UID of manager in question
                 }
    }


### remove pilots

Umgr informs Umgr.Scheduler about removed pilots

    {
        "cmd" : "rem_pilots",
        "arg" : {
                   "pilots": [],          # list of pilot dicts
                   "umgr"  : "umgr.0000"  # UID of manager in question
                }
    }


