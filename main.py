import click
import bris_workflow.action as action
import bris_workflow.execute as execute
from datetime import datetime, UTC

@click.group()
def cli():
    pass


@cli.command()
@click.option('--time', type=click.DateTime(), default=datetime(2025,7,1, 0, 0, 0, tzinfo=UTC), help='Time to get data for')
@click.option('--cascade', default='tcp://localhost:8067', help='Address of the Cascade server')
def run(time: datetime, cascade: str):
    '''Run a workflow, gathering and preparing data for a bris run.'''
    a = action.create(time=time)
    results = execute.action(a, cascade_address=cascade)
    print(results)

@cli.group()
def json():
    '''Commands for working with JSON actions.'''
    pass

@json.command()
@click.option('--time', type=click.DateTime(), default=datetime(2025,7,1, 0, 0, 0, tzinfo=UTC), help='Time to get data for')
def dump(time: datetime):
    a = action.create(time=time)
    print(execute.make_json(a))
    

@json.command()
@click.option('--cascade', default='tcp://localhost:8067', help='Address of the Cascade server')
@click.option('--no-wait', is_flag=True, help='Do not wait for the job to complete')
def submit(cascade: str, no_wait: bool):
    '''Read json from stdin and submit it to cascade.'''
    execute.submit_json(click.get_text_stream('stdin').read(), cascade, not no_wait)


def locally():
    import cascade.benchmarks.job1 as j1
    import cascade.benchmarks.distributed as di
    import cloudpickle

    spec = di.ZmqClusterSpec.local(j1.get_prob())
    print(spec.controller.outputs)
    # prints out:
    # {DatasetId(task='mean:dc9d90 ...
    # defaults to all "sinks", but can be overridden

    rv = di.launch_from_specs(spec, None)

    for key, value in rv.outputs.items():
        deser = cloudpickle.loads(value)
        print(f"output {key} is of type {type(deser)}")


if __name__ == "__main__":
    cli()
