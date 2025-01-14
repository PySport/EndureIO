from datetime import timezone
from itertools import pairwise
from os import PathLike
from typing import IO

import fitdecode
import pandas as pd


COLUMN_TRANSLATIONS = {
    'enhanced_speed': "speed",
    "position_lat": "latitude",
    "position_long": "longitude",
    "enhanced_altitude": "altitude",
    "saturated_hemoglobin_percent": "smo2",
    'Power': "power",  # From e.g. Stryd
    "Cadence": "cadence",  # From e.g. Stryd
    # "Leg Spring Stiffness": "leg_spring_stiffness",
    # "Vertical Oscillation": "vertical_oscillation",
}


OPINIATED_COLUMNS = {
    "timestamp",
    "sport",
    "sub_sport",
    "power",
    "speed",
    "distance",
    "longitude",
    "latitude",
    "altitude",
    "heart_rate",
    "cadence",
    "temperature",
    "core_temperature",
    "smo2",
}

FLOAT_COLUMNS = {
    "power",
    "speed",
    "distance",
    "heart_rate",
    "cadence",
    "temperature",
    "core_temperature",
    "smo2",
}


def _translate_columns(values, present_columns, allow_column_overwrites):
    """
    Translates the keys of a dictionary using the COLUMN_TRANSLATIONS dictionary.
    Only translates keys that are part of COLUMN_TRANSLATIONS and if the translated
    key is not already in the present_columns set.

    Parameters:
    - values (dict): The dictionary whose keys need to be translated.
    - present_columns (set): A set of column names that are already present and should not be overwritten.
    - allow_column_overwrites (bool): If True, allow opinionated translation of columns to overwrite existing columns.

    Returns:
    - dict: A dictionary with keys translated as per COLUMN_TRANSLATIONS, considering present_columns.
    """
    translated_values = {}
    for key, value in values.items():
        if key in COLUMN_TRANSLATIONS and (COLUMN_TRANSLATIONS[key] not in present_columns or allow_column_overwrites):
            translated_key = COLUMN_TRANSLATIONS[key]
        else:
            translated_key = key
        translated_values[translated_key] = value
    return translated_values


def _parse_device_info(device_info):
    """
    Parses the device information from the FIT file.
    """
    if device_info is None:
        return None

    if device_info["manufacturer"] == "garmin" and "garmin_product" in device_info:
        return f"garmin {device_info['garmin_product']}"
    elif "product_name" in device_info:
        return device_info["product_name"]
    elif "descriptor" in device_info:
        return device_info["descriptor"]
    elif device_info.get("device_name", None) is not None:
        return device_info["device_name"]
    else:
        return None


def read_fit(file_like: str | bytes | PathLike | IO[bytes], opinionated: bool = True, include_unopinionated: bool = True, allow_column_overwrites: bool = False) -> pd.DataFrame:
    """
    Reads a .fit file and returns a pandas DataFrame.

    Parameters:
    - file_like: A file-like object to read the .fit file from.
    - opinionated (bool, optional): If True, perform some opinionated data cleaning and transformation. For example "Power" is renamed to "power". Defaults to True.
    - include_unopinionated (bool, optional): If True, include columns that are not part of the opinionated columns. Defaults to True.
    - allow_column_overwrites (bool, optional): If True, allow opinionated translation of columns to overwrite existing columns. If false, then if for example a "power" column already exists, the "Power" column will not be renamed to "power". Defaults to False.

    Returns:
    - pandas.DataFrame: A DataFrame containing the data from the .fit file.
    """
    frames = []
    present_columns = set()
    sport = None
    sub_sport = None
    device_info = None
    with fitdecode.FitReader(file_like) as fit:
        laps = []
        sessions = []
        for frame in fit:
            if not isinstance(frame, fitdecode.records.FitDataMessage):
                continue

            if frame.mesg_type is None:
                continue

            match frame.mesg_type.name:
                case "record":
                    values = {
                        "sport": sport,
                        "sub_sport": sub_sport,
                    }
                    for f in frame.fields:
                        # @TODO this assumes 1 value per field name, but that's not necessarily the case.
                        values[f.name] = f.value

                    if opinionated:
                        present_columns.update(values.keys())
                        values = _translate_columns(values, present_columns, allow_column_overwrites)

                    if not include_unopinionated:
                        values = {key: value for key, value in values.items() if key in OPINIATED_COLUMNS}

                    frames.append(values)
                case "sport":
                    sport = frame.get_field("sport").value
                    sub_sport = frame.get_field("sub_sport").value
                case "lap":
                    laps.append(frame)
                case "session":
                    sessions.append(frame)
                case "device_info":
                    try:
                        device_index = frame.get_field("device_index").value
                    except Exception:
                        continue

                    if device_index == "creator":
                        device_info = {}
                        for field_name in ["device_index", "manufacturer", "device_name", "garmin_product", "product_name", "serial_number", "descriptor"]:
                            try:
                                device_info[field_name] = frame.get_field(field_name).value
                            except KeyError:
                                device_info[field_name] = None
                case "activity":
                    try:
                        local_timestamp = frame.get_field("local_timestamp").value
                        timestamp = frame.get_field("timestamp").value
                    except KeyError:
                        # Some FIT files don't have the local_timestamp field
                        tz = None
                    else:
                        tz = timezone(local_timestamp - timestamp.replace(tzinfo=None))
                case _:
                    pass

    data = pd.DataFrame(frames)

    data["timestamp"] = pd.to_datetime(data["timestamp"])
    if tz is not None:
        data["timestamp"] = data["timestamp"].dt.tz_convert(tz)
    data["duration"] = data["timestamp"].diff()
    data.set_index("timestamp", inplace=True)

    data.attrs["device"] = _parse_device_info(device_info)

    # @TODO Revisit if only manual lap triggers should be used (if present)
    # @TODO Support multi session FIT files
    lap_start_times = []
    for lap in laps:
        start_time = lap.get_value("start_time")

        try:
            lap_trigger = lap.get_value("lap_trigger")
        except KeyError:
            lap_trigger = None

        lap_start_times.append((start_time, lap_trigger))

    lap_idx = None
    for lap_idx, ((start, lap_trigger), (end, _)) in enumerate(pairwise(lap_start_times)):
        data.loc[start:end, "lap"] = lap_idx
        data.loc[start:end, "lap_trigger"] = lap_trigger

    if lap_idx is not None:
        data.loc[end:, "lap"] = lap_idx + 1
        data.loc[end:, "lap_trigger"] = "manual"  # The end of an activity is always triggered manually
    else:
        data["lap"] = None
        data["lap_trigger"] = None

    if opinionated:
        for original, translated in COLUMN_TRANSLATIONS.items():
            if original in data.columns and translated not in data.columns:
                data.rename(columns={original: translated}, inplace=True)

    for column in data.columns:
        if column in FLOAT_COLUMNS:
            data[column] = data[column].astype(float)

    return data