from earthkit.workflows import fluent
# from . import payload
from .payload import bris, graph, ifs, orography, reproject, state
from datetime import datetime


def create(time: datetime) -> fluent.Action:
    get_global_data_action = fluent.from_source([ifs.get(time)]) # type: ignore

    return get_global_data_action

    get_orography_action = fluent.from_source([orography.get()]) # type: ignore
    create_graph_action = fluent.from_source([graph.create()]) # type: ignore

    get_local_data_action = get_global_data_action\
        .join(get_orography_action, 'source')\
        .reduce(reproject.reproject(), dim='source') # type: ignore

    final_action = get_global_data_action\
        .join(get_local_data_action, 'source')\
        .join(create_graph_action, 'source')\
        .reduce(state.prepare(), dim='source') # type: ignore

    return final_action.map(bris.run()) # type: ignore
