from earthkit.workflows.decorators import as_payload
import earthkit.data as ekd
from datetime import datetime, time
from typing import Dict, Any, Optional
import os
import typing


@as_payload
def get(time: str) -> ekd.FieldList:
    output_dir = 'cache'
    reference_time = datetime.strptime(time, '%Y-%m-%dT%H:00:00')

    ret = None
    for ds in download(reference_time, output_dir):
        if ret is None:
            ret = ds
        else:
            ret += ds

    return ret # type: ignore

    # return 'ifs@' + time


_MAX_RESOLUTION = "N1280"  # Maximum resolution for mars data


def download(reference_time: datetime, output_dir: str) -> typing.Iterable[ekd.FieldList]:
    for level_type in "sfc", "lvl":
        grid_str = _MAX_RESOLUTION
        filename = os.path.join(
            output_dir,
            f'{output_dir}/global_{reference_time.strftime("%Y%m%d%H")}_{level_type}_{grid_str}.grib'
        )
        if os.path.exists(filename):
            yield ekd.from_source("file", filename) # type: ignore
            continue

        yield _download(reference_time, level_type)


def _download(reference_time: datetime, level_type: str) -> ekd.FieldList:
    grid = _MAX_RESOLUTION

    request = _get_request(reference_time, level_type, grid)

    # request['class'] = 'od'  # Needed since we use polytope
    # ds = ekd.from_source("polytope", "ecmwf-mars", request, read_all=True)
    ds = ekd.from_source("mars", request, read_all=True)
    
    return ds  # type: ignore


def _get_request(reference_time: datetime, level_type: str, grid, area: Optional[str] = None) -> Dict[str, Any]:
    """
    Prepare a request for ECMWF data based on the given time and level type.
    Will only fetch the first time step of the data.
    Args: 
        reference_time (datetime): Reference time for the data to request.
        level_type (str): Type of level, either 'sfc' for surface or 'lvl'
                        for pressure levels.
    Returns:
        Dict[str, Any]: A dictionary containing the request parameters.
    Raises: ValueError: If the time is not a multiple of 6.
    """

    if reference_time.time() not in (time(0), time(6), time(12), time(18)):
        raise ValueError('hour must be a multiple of 6')

    request: Dict[str, Any] = {
        "date": reference_time.strftime("%Y-%m-%d"),
        "time": reference_time.time().hour,
        "stream": "oper",
        "type": "fc",
        "step": [0],
    }
    if reference_time.time().hour % 12 == 6:
        request["stream"] = "scda"

    if grid is not None:
        request |= {
            "grid": grid,
        }
    if area is not None:
        request |= {
            "area": area,
        }

    if level_type == "sfc":
        request |= {
            "param": _SFC_PARAMETERS,
            "levtype": "sfc",
        }
    elif level_type == "lvl":
        request |= {
            "param": _LVL_PARAMETERS,
            "levtype": "pl",
            "levelist": _LEVELS,
        }

    return request


_SFC_PARAMETERS = ["2t", "2d", "skt", "tp", "10u", "10v", "z", "lsm", "tcw",
                   "sp", "msl", "lcc", "mcc", "hcc", "tcc", "3020", "10si", "cbh", "ssrd", "strd"]
_LVL_PARAMETERS = ["t", "q", "u", "v", "w", "z"]
_LEVELS = [50, 100, 150, 200, 250, 300, 400, 500, 700, 850, 925, 1000]
