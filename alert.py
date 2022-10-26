import sys

import click
from cli_passthrough import cli_passthrough
from cli_passthrough.utils import write_to_log

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

    sys.exit(exit_status)


if __name__ == "__main__":
    cli(obj={})
