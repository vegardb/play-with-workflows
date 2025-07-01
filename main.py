import click
import bris_workflow.action as action
import bris_workflow.execute as execute
from datetime import datetime, UTC


@click.command()
@click.option('--time', type=click.DateTime(), default=datetime(2025,7,1, 0, 0, 0, tzinfo=UTC), help='Time to get data for')
def cli(time: datetime):
    '''Run a workflow, gathering and preparing data for a bris run.'''
    a = action.create(time=time)
    results = execute.action(a)
    print(results)


if __name__ == "__main__":
    cli()
