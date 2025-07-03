import click
import bris_workflow.action as action
import bris_workflow.execute as execute
from datetime import datetime

@click.group()
def cli():
    pass


@cli.command()
@click.option('--time', type=click.DateTime(), default=datetime(2025,7,1, 0, 0, 0), help='Time to get data for')
@click.option('--cascade', default='tcp://localhost:8067', help='Address of the Cascade server')
@click.option('--no-wait', is_flag=True, help='Do not wait for the job to complete')
def run(time: datetime, cascade: str, no_wait: bool):
    '''Run a workflow, gathering and preparing data for a bris run.'''
    a = action.create(time=time)
    request, sinks = execute.get_submit_request(a)
    job_id = execute.submit_job(request, cascade)
    print(f"Job submitted with ID: {job_id}.")

    if no_wait:
        return

    try:
        execute.await_job(job_id, cascade)
    except Exception as e:
        print(e)
        return
    results = execute.get_job_results(job_id, sinks, cascade)

    for r in results:
        print(r, results[r])

@cli.group()
def json():
    '''Commands for working with JSON actions.'''
    pass

@json.command()
@click.option('--time', type=click.DateTime(), default=datetime(2025,7,1, 0, 0, 0), help='Time to get data for')
def dump(time: datetime):
    a = action.create(time=time)
    print(execute.make_json(a))
    

@json.command()
@click.option('--cascade', default='tcp://localhost:8067', help='Address of the Cascade server')
@click.option('--no-wait', is_flag=True, help='Do not wait for the job to complete')
def submit(cascade: str, no_wait: bool):
    '''Read json from stdin and submit it to cascade.'''
    execute.submit_json(click.get_text_stream('stdin').read(), cascade, not no_wait)



if __name__ == "__main__":
    cli()
