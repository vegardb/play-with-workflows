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
    request, _ = _get_submit_request(action)
    return request.model_dump_json(indent=2, warnings='error')


def submit_json(raw_json: str, cascade_address: str, wait: bool=True) -> JobId:
    request = api.SubmitJobRequest.model_validate_json(raw_json)
    return _submit_job(request, cascade_address, wait)


def action(action: fluent.Action, cascade_address: str = "tcp://localhost:8067"):
    '''Throwaway function to execute a workflow action and return the result.'''

    request, sinks = _get_submit_request(action)
    job_id = _submit_job(request, cascade_address)
    return _get_job_results(job_id, sinks, cascade_address)


def _get_submit_request(action: fluent.Action) -> typing.Tuple[api.SubmitJobRequest, set[DatasetId]]:
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


def _submit_job(request: api.SubmitJobRequest, cascade_address: str, wait=True) -> JobId:
    submit_job_response: api.SubmitJobResponse = client.request_response(
        request, cascade_address)  # type: ignore
    if submit_job_response.error is not None:
        err = submit_job_response.error
        raise RuntimeError(f"Failed to submit job: {err}")
    if submit_job_response.job_id is None:
        raise RuntimeError("Job submission did not return a job ID")

    job_id = submit_job_response.job_id

    if not wait:
        return job_id
    
    available_request = api.JobProgressRequest(job_ids=[job_id])
    done = False
    while not done:
        time.sleep(1)
        availability: api.JobProgressResponse = client.request_response(
            available_request, cascade_address)  # type: ignore

        if availability.error is not None:
            raise RuntimeError(f'Job failed: {availability.error}')

        done = availability.progresses[job_id].pct == '100.00'

    return job_id


def _get_job_results(job_id: JobId, sinks: set[DatasetId], cascade_address: str) -> api.ResultRetrievalResponse:
    result_request = api.ResultRetrievalRequest(
        job_id=job_id, dataset_id=list(sinks)[0])
    response: api.ResultRetrievalResponse = client.request_response(
        result_request, cascade_address)  # type: ignore
    return api.decoded_result(response, job=None)  # type: ignore
