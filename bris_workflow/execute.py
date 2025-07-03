import typing
from earthkit.workflows import fluent, Cascade
from cascade.controller.report import JobId, JobProgress
from cascade.low.into import graph2job
from cascade.low import views as cascade_views
import cascade.gateway.api as api
import cascade.gateway.client as client
from cascade.low.core import DatasetId
import time


def make_json(action: fluent.Action) -> str:
    '''Convert a workflow action to a JSON string for submission.'''
    request, _ = get_submit_request(action)
    return request.model_dump_json(indent=2, warnings='error')


def submit_json(raw_json: str, cascade_address: str, wait: bool=True) -> JobId:
    request = api.SubmitJobRequest.model_validate_json(raw_json)
    job_id = submit_job(request, cascade_address)
    if wait:
        await_job(job_id, cascade_address)
    return job_id


def get_submit_request(action: fluent.Action) -> typing.Tuple[api.SubmitJobRequest, set[DatasetId]]:
    graph = Cascade(action.graph())
    job = graph2job(graph._graph)
    sinks = cascade_views.sinks(job)
    job.ext_outputs = list(sinks)
    job = api.JobSpec(
        benchmark_name=None,
        workers_per_host=1,
        hosts=1,
        envvars={},
        use_slurm=False,
        job_instance=job,
    )
    return api.SubmitJobRequest(job=job), sinks


def submit_job(request: api.SubmitJobRequest, cascade_address: str) -> JobId:
    submit_job_response: api.SubmitJobResponse = client.request_response(
        request, cascade_address)  # type: ignore
    if submit_job_response.error is not None:
        err = submit_job_response.error
        raise RuntimeError(f"Failed to submit job: {err}")
    if submit_job_response.job_id is None:
        raise RuntimeError("Job submission did not return a job ID")

    job_id = submit_job_response.job_id
    return job_id


def await_job(job_id: JobId, cascade_address: str):
    """Wait for a job to complete."""
    available_request = api.JobProgressRequest(job_ids=[job_id])
    done = False
    while not done:
        time.sleep(1)
        resp: api.JobProgressResponse = client.request_response(
            available_request, cascade_address)  # type: ignore

        status = resp.progresses[job_id]

        # if resp.error is not None:
        #     raise RuntimeError(f'Job failed: {resp.error}')

        if status.failure:
            print() # Add a newline, bacause of the complete \r trick, below
            raise RuntimeError(f'Job {job_id} failed: {status.failure}')
        
        print(f'\r{status.pct}% complete', end='')

        done = status.pct == '100.00'
    print()

def get_job_results(job_id: JobId, sinks: set[DatasetId], cascade_address: str) -> typing.Dict[DatasetId, api.ResultRetrievalResponse]:
    ret = {}
    for s in sinks:
        result_request = api.ResultRetrievalRequest(
            job_id=job_id, dataset_id=s)
        response: api.ResultRetrievalResponse = client.request_response(
            result_request, cascade_address)  # type: ignore
        ret[s] = api.decoded_result(response, job=None)  # type: ignore
    return ret