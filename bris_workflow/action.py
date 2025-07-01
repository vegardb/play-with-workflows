from earthkit.workflows import fluent
from . import payload
from datetime import datetime


def create(time: datetime) -> fluent.Action:
    get_global_data_action = fluent.from_source([payload.get_ifs(time)]) # type: ignore
    get_orography_action = fluent.from_source([payload.get_orography()]) # type: ignore
    create_graph_action = fluent.from_source([payload.create_graph()]) # type: ignore

    get_local_data_action = get_global_data_action\
        .join(get_orography_action, 'source')\
        .reduce(payload.reproject(), dim='source') # type: ignore

    final_action = get_global_data_action\
        .join(get_local_data_action, 'source')\
        .join(create_graph_action, 'source')\
        .reduce(payload.prepare_state(), dim='source') # type: ignore

    return final_action.map(payload.run_bris()) # type: ignore
