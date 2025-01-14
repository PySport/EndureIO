from os import PathLike
from typing import IO

import pandas as pd


def read_fit(file_path_or_buffer: str | bytes | PathLike | IO[bytes]) -> pd.DataFrame:
    pass