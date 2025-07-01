from earthkit.workflows import fluent, Cascade
from cascade.low.into import graph2job
from cascade.low import views as cascade_views
import cascade.gateway.api as api
import cascade.gateway.client as client
import time


def action(action: fluent.Action, cascade_address: str = "tcp://localhost:8067"):
    '''Throwaway function to execute a workflow action and return the result.'''
    
    graph = Cascade(action.graph())

    job = graph2job(graph._graph)
    sinks = cascade_views.sinks(job)
    job.ext_outputs = list(sinks)

    r = api.SubmitJobRequest(
        job=api.JobSpec(
            benchmark_name=None, workers_per_host=1,
            hosts=1, envvars={}, use_slurm=False, job_instance=job)
    )
    submit_job_response: api.SubmitJobResponse = client.request_response(
        r, cascade_address)  # type: ignore
    if submit_job_response.error is not None:
        err = submit_job_response.error
        raise RuntimeError(f"Failed to submit job: {err}")
    if submit_job_response.job_id is None:
        raise RuntimeError("Job submission did not return a job ID")

    job_id = submit_job_response.job_id

    available_request = api.JobProgressRequest(job_ids=[job_id])
    done = False
    while not done:
        time.sleep(1)
        availability: api.JobProgressResponse = client.request_response(
            available_request, cascade_address)  # type: ignore

        if availability.error is not None:
            raise RuntimeError(f'Job failed: {availability.error}')

        done = availability.progresses[job_id].pct == '100.00'

    result_request = api.ResultRetrievalRequest(
        job_id=job_id, dataset_id=list(sinks)[0])
    response: api.ResultRetrievalResponse = client.request_response(
        result_request, cascade_address)  # type: ignore
    result = api.decoded_result(response, job=None) # type: ignore

    return result
