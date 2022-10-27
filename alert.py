import sys

import click
from cli_passthrough import cli_passthrough
from cli_passthrough.utils import write_to_log
import requests

CONTEXT_SETTINGS = {"ignore_unknown_options": True, "allow_extra_args": True}


# CLI entry point
@click.command(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def cli(ctx):
    """Entry point"""
    write_to_log("\nNEW CMD = {}".format(" ".join(sys.argv[1:])))
    write_to_log("\nNEW CMD = {}".format(" ".join(sys.argv[1:])), "stderr")

    exit_status = cli_passthrough(" ".join(ctx.args), interactive=False)

    # TODO - call code to parse dbt results and send slack alerts here.
    url = 'https://hooks.slack.com/services/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'
    alert_text = ""
    with open("./logs/history.log", 'r') as f:
        for line in f:
            if "Failure in test" in line:
                alert_text += line + '\n'
    myobj = {"text": alert_text}

    x = requests.post(url, json = myobj)

    sys.exit(exit_status)


if __name__ == "__main__":
    cli(obj={})
