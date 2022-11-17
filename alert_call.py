import sys

import click
from cli_passthrough import cli_passthrough
from cli_passthrough.utils import write_to_log
import requests
import json

CONTEXT_SETTINGS = {"ignore_unknown_options": True, "allow_extra_args": True}

def create_message(**kwargs):
    messageBody = {
    "text": "Hey <@U01TNM2CZLG>, new failed DBT tests", 
    "attachments": [ 
        {
        "color": "#f44336",
        "fields": [
            {
            "title": "Invocation ID",
            "value": kwargs["invocation_id"],
            "short": True
            },
            {
            "title": "Failed Table",
            "value": kwargs["table_name"],
            "short": True
            },
            {
            "title": "Environment",
            "value": "Development",
            "short": True
            },
            {
            "title": "Failed Timestamp",
            "value": kwargs["timestamp"],
            "short": True
            },
            {
            "title": "Please take actions to deal with that failed tests",
            "value": "Please take the following actions to deal with this failed tests",
            "short": False 
            }
        ],
        "actions": [ 
            {
            "type": "button",
            "text": "Take a look",
            "url": "http://example.com" 
            },
            {
            "type": "button",
            "text": "Fixed",
            "style": "primary", 
            "url": "http://example.com"
            },
            {
            "type": "button",
            "text": "Ignore and walk away",
            "style": "danger",
            "url": "http://example.com/order/1/cancel",
            "confirm": {
                "title": "Sorry there is no way to ignore and walk away",
                "text": "Choose again, now only 2 option",
                "ok_text": "Take a look",
                "dismiss_text": "Fixed"
            }
            }
        ]
        }
    ]
    }

    return messageBody

# CLI entry point
@click.command(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def cli(ctx):
    """Entry point"""
    write_to_log("\nNEW CMD = {}".format(" ".join(sys.argv[1:])))
    write_to_log("\nNEW CMD = {}".format(" ".join(sys.argv[1:])), "stderr")

    print()

    exit_status = cli_passthrough(" ".join(ctx.args), interactive=False)

    # TODO - call code to parse dbt results and send slack alerts here.
    url = 'https://hooks.slack.com/services/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'
    alert_text = ""
    f = open('./run_results.json')
    data = json.load(f)
    failed_message = [x for x in data["results"] if x["status"] != "pass"]
    for message in failed_message:
        alert_text = alert_text + (str(message) + "\n" + "@Xiuyang")

    send_message = create_message(
        table_name=data["args"]["select"][0],
        invocation_id=data["metadata"]["invocation_id"],
        timestamp=data["metadata"]["generated_at"]
    )
    x = requests.post(url, json = send_message)

    sys.exit(exit_status)

if __name__ == "__main__":
    cli(obj={})