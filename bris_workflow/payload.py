from earthkit.workflows.decorators import as_payload
from datetime import datetime

@as_payload
def get_ifs(time: str):
    return 'ifs@' + time


@as_payload
def get_orography():
    return 'orography'


@as_payload
def reproject(get_ifs, get_orography):
    return f'reprojected {get_ifs} with {get_orography}'


@as_payload
def create_graph():
    return 'graph'


@as_payload
def prepare_state(global_data, local_data, graph):
    return f'prepared state with {global_data}, {local_data}, and {graph}'


@as_payload
def run_bris(state):
    return f'Running BRIS with state: {state}'
