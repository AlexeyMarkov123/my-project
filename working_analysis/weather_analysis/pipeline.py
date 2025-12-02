import math
import datetime
from dateutil.tz import tzoffset
import json
import os
from collections import defaultdict
import time
from time import time as now
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys

import pickle
import numpy as np
import pandas as pd
import yaml
from shapely import wkt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon


class Pipeline:
    def __init__(self, yaml_file_name):
        self.yaml_file_name = yaml_file_name
        yaml_file_name = yaml_file_name.replace(".\\", "")
        self.spec = yaml.safe_load(open(yaml_file_name, "rt"))
        self.output_json_file_name = yaml_file_name.replace(".yml", "_outout.json")
        self.polygons = defaultdict(self.empty_dict)
        self.points = defaultdict(self.empty_dict)
        self.aggregations = defaultdict(self.empty_dict)
        self.intervals = None
        self.max_workers = 60
        self.max_agg_day_count = 9999999

    def empty_dict():
        return {}

    def load_parquet_file(self, data_file_name, path=""):
        try:
            if data_file_name[0] == "/" or data_file_name[1:3] == ":\\":
                file_name = data_file_name
            else:
                file_name = os.path.join(path, data_file_name)
            df = pd.read_parquet(file_name)
            return df
        except:
            path_to_show = path + "/" if path > "" else ""
            print(f"ERROR: Could not load file {path_to_show}{data_file_name}.")
            return None

    def _convert_date(self, df):
        if not isinstance(df["validdate"][0], datetime.date):
            df["validdate2"] = df["validdate2"].apply(lambda d: datetime.date(d))
            df.drop(columns=["validdate"], inplace=True)
            return df.rename(columns={"validdate2": "validdate"})
        else:
            return df

    def load_data(self, path="", data_file_name=None, location=None):
        print('load_data')
        
        for loc_name, loc_spec in self.spec["locations"].items():
            data_file_name = loc_spec["data_file_name"]
            print(data_file_name)
            if ".parquet" in data_file_name:
                print(data_file_name)
                loc_df = self.load_parquet_file(data_file_name)
                if loc_df is None:
                    continue
                for field_name in self.spec["derived_fields"]:
                    loc_df[field_name] = self.compute_derived_field(field_name, loc_df)
                loc_df.sort_values(by=["validdate"])
                loc_df.to_parquet(f"tmp_{loc_name}.parquet")

            if "areas" in loc_spec:
                self._get_inside_lat_lon(loc_df, loc_name, loc_spec, path)

            if "additional_files" in loc_spec:
                print("additional_files")
                additional_files = loc_spec["additional_files"]
                for i, file_name in enumerate(additional_files):
                    print(i)
                    print(f'Loading file #{i+2}: "{file_name}".')
                    df = self.load_parquet_file(file_name)
                    if df is None:
                        continue
                    loc_df2 = pd.concat([loc_df, df], axis=0, ignore_index=True)
                    del df
                    del loc_df
                    loc_df = loc_df2
                

            for field_name in self.spec["derived_fields"]:
                loc_df[field_name] = self.compute_derived_field(field_name, loc_df)

            loc_df.sort_values(by=["validdate"])
            loc_df.to_parquet(f"tmp_{loc_name}.parquet")
	    
            points_file_name = (
                f"{data_file_name.replace('.parquet','_points_')}_{loc_name}.json"
            )
            json.dump(self.points, open(points_file_name, "wt"))
            self._get_timezones()

    def _get_timezones(self):
        self.timezones = {}
        for loc_name, loc_spec in self.spec["locations"].items():
            self.timezones[loc_name] = {}
            if "timezones" in loc_spec:
                for zone_name, zone_spec in loc_spec["timezones"].items():
                    delta_str = zone_spec["delta"]
                    if ":" in delta_str:
                        timezone_hours = int(delta_str[: delta_str.find(":")])
                        timezone_mins = int(delta_str[delta_str.find(":") + 1 :])
                    else:
                        timezone_hours = int(delta_str)
                        timezone_mins = 0
                    self.timezones[loc_name][zone_name] = {
                        "start_date_time_str": zone_spec["start_date_time"],
                        "end_date_time_str": zone_spec["end_date_time"],
                        "delta_str": delta_str,
                        "start_date_time": datetime.datetime.strptime(
                            zone_spec["start_date_time"], "%Y-%m-%d %H:%M"
                        ),
                        "end_date_time": datetime.datetime.strptime(
                            zone_spec["end_date_time"], "%Y-%m-%d %H:%M"
                        ),
                        "timezone_hours": timezone_hours,
                        "timezone_mins": timezone_mins,
                    }

    def _tz_hrs_mins(self, loc_name, date_time):
        for zone_name, zone_spec in self.timezones[loc_name].items():
            start_date_time = zone_spec["start_date_time"]
            end_date_time = zone_spec["end_date_time"]
            if start_date_time < date_time < end_date_time:
                return (
                    zone_name,
                    zone_spec["timezone_hours"],
                    zone_spec["timezone_mins"],
                )
        return "Unknown", 0, 0

    def _get_inside_lat_lon(self, loc_df, loc_name, loc_spec, path):
        areas = loc_spec["areas"]
        self.polygons[loc_name] = {}
        self.points[loc_name] = defaultdict(self.empty_dict)
        for area in areas:
            self.polygons[loc_name][area] = {"include": [], "exclude": []}
            include_files = areas[area]["include"] if "include" in areas[area] else []
            exclude_files = areas[area]["exclude"] if "exclude" in areas[area] else []
            for geo_file_name in include_files:
                if geo_file_name:
                    shape_file_name = os.path.join(path, geo_file_name)
                    if not os.path.exists(shape_file_name):
                        print(f'ERROR: Could not find file "{shape_file_name}".')
                        continue
                    try:
                        polygon = wkt.loads(open(shape_file_name).read())
                        self.polygons[loc_name][area]["include"].append(polygon)
                    except:
                        print(f'ERROR: Could not load file "{shape_file_name}".')
            for geo_file_name in exclude_files:
                if geo_file_name:
                    shape_file_name = os.path.join(path, geo_file_name)
                    if not os.path.exists(shape_file_name):
                        print(f'ERROR: Could not find file "{shape_file_name}".')
                        continue
                    try:
                        polygon = wkt.loads(open(shape_file_name).read())
                        self.polygons[loc_name][area]["exclude"].append(polygon)
                    except:
                        print(f'ERROR: Could not load file "{shape_file_name}".')
            self.points[loc_name][area] = []
            df_lat_lon = loc_df[["lon", "lat"]].drop_duplicates()
            unique_points = [(r[0], r[1]) for _, r in df_lat_lon.iterrows()]
            for xy in unique_points:
                inside = not self.polygons[loc_name][area]["include"]
                for polygon in self.polygons[loc_name][area]["include"]:
                    if polygon.contains(Point(xy)):
                        inside = True
                        break
                if inside:
                    for polygon in self.polygons[loc_name][area]["exclude"]:
                        if polygon.contains(Point(xy)):
                            inside = False
                            break
                if inside:
                    self.points[loc_name][area].append(xy)

    def apply_sum_of_last_rule(
        self, loc_df, source_field_name, new_field_name, window_size
    ):
        loc_df[new_field_name] = 0.0
        average_initial = loc_df[source_field_name][window_size] / window_size
        loc_df.loc[0:window_size, new_field_name] = average_initial
        win_data = np.array(loc_df[: window_size - 1][new_field_name])
        win_data[0] = 0
        for idx, row in loc_df[window_size:].iterrows():
            new_value = row[source_field_name] - np.sum(win_data)
            loc_df.at[idx, new_field_name] = new_value
            win_data = np.roll(win_data, -1)
            win_data[-1] = new_value

    def get_areas(self, location):
        areas = ["all"]
        if "areas" in self.spec["locations"][location]:
            areas += list(self.spec["locations"][location]["areas"])
        return areas

    def compute_derived_field(self, derived_field_name, df):
        cat_field_spec, num_field_name = derived_field_name.split("&")
        cat_field_spec, num_field_name = cat_field_spec.strip(), num_field_name.strip()
        cat_field_name, cat_field_value_str = cat_field_spec.split("==")
        cat_field_name, cat_field_value_str = (
            cat_field_name.strip(),
            cat_field_value_str.strip(),
        )
        cat_field_value = int(cat_field_value_str)
        return df[df[cat_field_name] == cat_field_value][num_field_name]

    def _process_dates(self, df_geo_segment, unique_dates, area):
        # print(f'Processing {len(unique_dates)} dates for {area}.')
        try:
            start = now()
            area_agg = {}
            for date in unique_dates:
                date_str = date.isoformat()
                fields_to_drop = ["validdate"]
                if "location" in df_geo_segment.columns:
                    fields_to_drop += ["location"]
                df_segment = df_geo_segment[
                    df_geo_segment["validdate"] == date_str
                ].drop(fields_to_drop, axis=1)
                if len(df_segment) == 0:
                    continue
                area_agg[date_str] = defaultdict(self.empty_dict)
                for agg in self.spec["aggregations"]:
                    if agg.startswith("min"):
                        area_agg[date_str][agg] = df_segment.min().to_dict()
                    elif agg.startswith("ave"):
                        area_agg[date_str][agg] = df_segment.mean().to_dict()
                    elif agg.startswith("max"):
                        area_agg[date_str][agg] = df_segment.max().to_dict()
                    elif agg.startswith("pct"):
                        area_agg[date_str][agg] = df_segment.quantile(
                            float(agg[3:]) / 100
                        ).to_dict()
                del df_segment

            print(
                f"Completed processing {len(unique_dates)} dates for {area} within {now()-start:.1f} secs."
            )
            return area, area_agg
        except Exception as e:
            print(
                f"ERROR: Failed to process dates for area {area} for {len(unique_dates)} dates and {len(df_geo_segment)} rows.\n{e}"
            )

    def _get_area_segment(self, df, points, area):
        try:
            start = now()
            dfpts = []
            for xy in points:
                df1 = df[(np.fabs(df["lon"] - xy[0]) < 1e-5)]
                df2 = df1[(np.fabs(df1["lat"] - xy[1]) < 1e-5)]
                del df1
                dfpts.append(df2)
            out_df = pd.concat(dfpts, axis=0)
            for df in dfpts:
                del df
            print(
                f"Completed shape filter for {area} with {len(out_df)} rows within {now()-start:.1f} secs"
            )
            return area, out_df
        except Exception as e:
            print(
                f"ERROR: Failed to process shapes for area {area} and points {str(points)}\n{e}"
            )

    def compute_aggregations(self, location=None):
        print("compute_aggregations")
        if location:
            locations = [location]
        else:
            locations = list(self.spec["locations"])

        for location in locations:
            print(f"Loading data for location {location}.")

            df = pd.read_parquet(f"tmp_{location}.parquet")
            self.aggregations[location] = defaultdict(self.empty_dict)
            unique_dates = df["validdate"].unique()
            n_dates = min(len(unique_dates), self.max_agg_day_count)

            start = now()
            futures = []
            pool = ProcessPoolExecutor(max_workers=self.max_workers)
            pool_shapes = ProcessPoolExecutor(max_workers=self.max_workers)

            dates_batch_size = 500
            date_partitions = []
            for i in range(len(unique_dates) // dates_batch_size + 1):
                if len(unique_dates) >= i * dates_batch_size:
                    dates = unique_dates[
                        i * dates_batch_size : (i + 1) * dates_batch_size
                    ]
                    date_partitions.append(
                        [pd.to_datetime(d).to_pydatetime() for d in dates]
                    )

            shape_futures = []
            for area in self.get_areas(location):
                start = now()

                self.aggregations[location][area] = defaultdict(self.empty_dict)
                if area == "all":
                    df.to_parquet(f"tmp_{location}_{area}.parquet")
                else:
                    area_point = self.points[location][area]
                    shape_batch_size = 500000
                    for i in range(len(df) // shape_batch_size + 1):
                        if len(df) >= i * shape_batch_size:
                            shapes_batch_df = df[
                                i * shape_batch_size : (i + 1) * shape_batch_size
                            ]
                            shape_futures.append(
                                pool_shapes.submit(
                                    self._get_area_segment,
                                    shapes_batch_df,
                                    area_point,
                                    area,
                                )
                            )

            print(f"Retrieving shape filter results for {len(shape_futures)} futures.")
            temp_area_dfs = {}
            for future in as_completed(shape_futures, timeout=None):
                area_name, area_df = future.result()
                if len(area_df) == 0:
                    continue
                if area_name not in temp_area_dfs:
                    temp_area_dfs[area_name] = area_df
                else:
                    temp_area_dfs[area_name] = pd.concat(
                        [temp_area_dfs[area_name], area_df], axis=0
                    )
                    del area_df

            print(f"Filtering areas completed within {now()-start:.1f} secs.")

            start = now()
            for area in self.get_areas(location):
                df_geo_segment = pd.read_parquet(f"tmp_{location}_{area}.parquet")
                for i, dates in enumerate(date_partitions):

                    def submit():
                        df_to_submit = df_geo_segment[
                            df_geo_segment["validdate"].isin(dates)
                        ]
                        futures.append(
                            pool.submit(
                                self._process_dates,
                                df_to_submit,
                                dates,
                                area,
                            )
                        )

                    retry_count = 3
                    while retry_count > 0:
                        try:
                            submit()
                            if retry_count < 3:
                                print(
                                    f"INFO: Retry #{4-retry_count} successul for {location}-{area} partition #{i}."
                                )
                            retry_count = 0
                        except:
                            retry_count -= 1
                            time.sleep(10)
                            if retry_count > 0:
                                print(
                                    f"WARNING: Retry #{3-retry_count} to load and submit data for {location}-{area} partition #{i}."
                                )
                            else:
                                print(
                                    f"ERROR: Failed to load and submit data for {location}-{area} partition #{i}."
                                )

            print(f"Submitted date jobs within {now()-start:.1f} secs.")

            start = now()
            aggs = []
            for future in as_completed(futures, timeout=None):
                aggs.append(future.result())
            for area, agg in aggs:
                self.aggregations[location][area].update(agg)
            print(f"Completed date iteration jobs within {now()-start:.1f} secs.")

    def save_aggregate_data(self, base_file_name, location=None):
        print("save_aggregate_data")
        start = now()
        data = []
        for location_name, location_data in self.aggregations.items():
            for area_name, area_data in location_data.items():
                for date_time in sorted(area_data):
                    agg_data = area_data[date_time]
                    for agg_name, field_data in agg_data.items():
                        for field_name, field_value in field_data.items():
                            data.append(
                                [
                                    location_name,
                                    area_name,
                                    date_time,
                                    agg_name,
                                    field_name,
                                    field_value,
                                ]
                            )
                df = pd.DataFrame(
                    data=data,
                    columns=[
                        "location_name",
                        "area_name",
                        "date_time",
                        "agg_name",
                        "field_name",
                        "field_value",
                    ],
                )
                df.to_parquet(
                    f'{base_file_name.replace(".parquet",f"_{location_name}_{area_name}.parquet")}'
                )
                print(
                    f"Saved {len(df)} rows to {base_file_name} within {now()-start:.1f} secs."
                )
                data = []

    def load_aggregate_data(self, base_file_name, location=None):
        self.aggregations = defaultdict(self.empty_dict)
        self._get_timezones()
        for location_name, loc_spec in self.spec["locations"].items():
            self.aggregations[location_name] = {}
            for area_name in self.get_areas(location_name):
                df = pd.read_parquet(
                    base_file_name.replace(
                        ".parquet", f"_{location_name}_{area_name}.parquet"
                    )
                )
                self.aggregations[location_name][area_name] = defaultdict(
                    self.empty_dict
                )
                for _, row in df.iterrows():
                    date_time = row["date_time"]
                    agg_name = row["agg_name"]
                    if date_time not in self.aggregations[location_name][area_name]:
                        self.aggregations[location_name][area_name][
                            date_time
                        ] = defaultdict(self.empty_dict)
                    if (
                        agg_name
                        not in self.aggregations[location_name][area_name][date_time]
                    ):
                        self.aggregations[location_name][area_name][date_time][
                            agg_name
                        ] = defaultdict(self.empty_dict)
                    self.aggregations[row["location_name"]][row["area_name"]][
                        date_time
                    ][agg_name][row["field_name"]] = row["field_value"]

    def get_interval_aggregates(
        self, location_name, area_name, event_name, start_date_str, end_date_str
    ):
        aggs = self.aggregations[location_name][area_name]
        agg_names = self.spec["aggregations"]
        interval_data = []
        event_spec = self.spec["locations"][location_name]["events"][event_name]
        low_field_name = (
            event_spec["low_threshold_field"]
            if "low_threshold_field" in event_spec
            else None
        )
        high_field_name = (
            event_spec["high_threshold_field"]
            if "high_threshold_field" in event_spec
            else None
        )
        agg_name_low = [
            (low_field_name if low_field_name else "no_low") + "-" + agg_name
            for agg_name in agg_names
        ]
        agg_name_high = [
            (high_field_name if high_field_name else "no_high") + "-" + agg_name
            for agg_name in agg_names
        ]
        for date_time, agg_data in aggs.items():
            if date_time >= start_date_str and date_time < end_date_str:
                if low_field_name:
                    row_low = [vals[low_field_name] for _, vals in agg_data.items()]
                else:
                    row_low = [None] * len(agg_names)
                if high_field_name:
                    row_high = [vals[high_field_name] for _, vals in agg_data.items()]
                else:
                    row_high = [None] * len(agg_names)
                interval_data.append(row_low + row_high)
        if agg_names is not None:
            return pd.DataFrame(
                data=interval_data, columns=agg_name_low + agg_name_high
            )
        else:
            return None

    def get_interval_start_end(
        self, event_start, dt_prev, downtime_before, downtime_after
    ):
        dt_start = datetime.datetime.strptime(event_start, "%Y-%m-%dT%H:%M:%S")
        dt_start -= datetime.timedelta(
            hours=downtime_before.hour,
            minutes=downtime_before.minute,
            seconds=downtime_before.second,
        )
        dt_end = datetime.datetime.strptime(dt_prev, "%Y-%m-%dT%H:%M:%S")
        dt_end += datetime.timedelta(
            hours=downtime_after.hour,
            minutes=downtime_after.minute,
            seconds=downtime_after.second,
        )
        return dt_start.strftime("%Y-%m-%dT%H:%M:%S"), dt_end.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    def _normalize_date(self, date_str):
        if "+" in date_str:
            date_str = date_str[: date_str.find("+")]
        if " " in date_str:
            date_str = date_str.replace(" ", "T")
        return date_str

    def _add_event(
        self,
        location,
        area,
        event_name,
        event_start,
        dt_prev,
        downtime_before,
        downtime_after,
        raw_intervals,
        raw_intervals_without_downtime,
    ):
        try:
            event_end = (
                datetime.datetime.strptime(dt_prev, "%Y-%m-%dT%H:%M:%S")
                + datetime.timedelta(minutes=5)
            ).strftime("%Y-%m-%dT%H:%M:%S")
            dt_start, dt_end = self.get_interval_start_end(
                event_start,
                event_end,
                downtime_before,
                downtime_after,
            )
            raw_intervals.append([dt_start, dt_end])
            raw_intervals_without_downtime.append([event_start, event_end])
        except:
            print(
                f"ERROR: Could not add interval for {event_start,dt_prev,downtime_before,downtime_after}"
            )
            breakpoint()

        print("===")
        print(f"Ended event {location} {area} {event_name} at {event_end}.")
        return event_start, event_end

    def compute_events(self, location=None, log_prefix=""):
        print("compute_events")
        if not self.aggregations:
            raise Exception(
                "ERROR: Missing aggregations required for computing events."
            )
        if location:
            locations = [location]
        else:
            locations = list(self.spec["locations"])

        self.intervals = defaultdict(self.empty_dict)
        self.intervals_without_downtime = defaultdict(self.empty_dict)
        for location in locations:
            if location not in self.spec["locations"]:
                continue
            self.intervals[location] = defaultdict(self.empty_dict)
            self.intervals_without_downtime[location] = defaultdict(self.empty_dict)
            for area in self.get_areas(location):
                self.intervals[location][area] = defaultdict(self.empty_dict)
                self.intervals_without_downtime[location][area] = defaultdict(
                    self.empty_dict
                )
                for event_name, event_spec in self.spec["locations"][location][
                    "events"
                ].items():
                    self.intervals[location][area][event_name] = []
                    self.intervals_without_downtime[location][area][event_name] = []
                    raw_intervals = []
                    raw_intervals_without_downtime = []
                    print(
                        f'{log_prefix} Processing event "{event_name}" for location: "{location}".'
                    )
                    event_state, event_start = False, None
                    low_field_name = (
                        event_spec["low_threshold_field"]
                        if "low_threshold_field" in event_spec
                        else None
                    )
                    low_agg_name = (
                        event_spec[f"low_threshold_agg"]
                        if "low_threshold_agg" in event_spec
                        else None
                    )
                    low_threshold = (
                        event_spec[f"low_threshold_value"]
                        if "low_threshold_value" in event_spec
                        else None
                    )
                    if low_threshold and not isinstance(low_threshold, float):
                        low_threshold = float(low_threshold)
                    high_field_name = (
                        event_spec["high_threshold_field"]
                        if "high_threshold_field" in event_spec
                        else None
                    )
                    high_agg_name = (
                        event_spec[f"high_threshold_agg"]
                        if "high_threshold_agg" in event_spec
                        else None
                    )
                    high_threshold = (
                        event_spec[f"high_threshold_value"]
                        if "high_threshold_value" in event_spec
                        else None
                    )
                    if high_threshold and not isinstance(high_threshold, float):
                        high_threshold = float(high_threshold)
                    downtime_before = datetime.datetime.strptime(
                        event_spec["downtime_before"], "%H:%M:%S"
                    )
                    downtime_after = datetime.datetime.strptime(
                        event_spec["downtime_after"], "%H:%M:%S"
                    )
                    aggs = self.aggregations[location][area]
                    for dt, vals in aggs.items():
                        low_val = None
                        if low_agg_name:
                            low_aggs = vals[low_agg_name]
                            if low_field_name in low_aggs:
                                low_val = (
                                    vals[low_agg_name][low_field_name]
                                    if low_agg_name
                                    else None
                                )
                        high_val = None
                        if high_agg_name:
                            high_aggs = vals[high_agg_name]
                            if high_field_name in high_aggs:
                                high_val = (
                                    vals[high_agg_name][high_field_name]
                                    if high_agg_name
                                    else None
                                )

                        dt = self._normalize_date(dt)
                        if (
                            (
                                low_threshold is not None
                                and low_val is not None
                                and (low_val < low_threshold)
                            )
                            or (
                                high_threshold is not None
                                and high_val is not None
                                and (high_val > high_threshold)
                            )
                        ) and not event_state:
                            print("===")
                            print(
                                f"Started event {location} {area} {event_name} at {dt}."
                            )
                            print("===")
                            event_start = dt
                            event_state = True
                        elif event_state and (
                            (
                                not (
                                    low_threshold is not None
                                    and low_val is not None
                                    and (low_val < low_threshold)
                                )
                                and not (
                                    high_threshold is not None
                                    and high_val is not None
                                    and (high_val > high_threshold)
                                )
                            )
                            or (low_val is not None and math.isnan(low_val))
                            or (high_val is not None and math.isnan(high_val))
                        ):
                            self._add_event(
                                location,
                                area,
                                event_name,
                                event_start,
                                dt_prev,
                                downtime_before,
                                downtime_after,
                                raw_intervals,
                                raw_intervals_without_downtime,
                            )
                            event_state = False

                        print(f"--- {dt} {event_name} {area} {location} ---")
                        print(f"low_val={low_val},   low_threshold={low_threshold}")
                        print(f"high_val={high_val},   high_threshold={high_threshold}")
                        dt_prev = dt

                    if event_start and event_state:
                        self._add_event(
                            location,
                            area,
                            event_name,
                            event_start,
                            dt_prev,
                            downtime_before,
                            downtime_after,
                            raw_intervals,
                            raw_intervals_without_downtime,
                        )

                    print("---")
                    print(
                        f"Done computing intervals; next merging for {event_name} {area} {location}."
                    )
                    if len(raw_intervals) > 0:
                        running_interval = raw_intervals[0]
                        for interval in raw_intervals[1:]:
                            if (
                                running_interval[1] >= interval[0]
                                and running_interval[1] < interval[1]
                            ):
                                running_interval[1] = interval[1]
                                print(
                                    f"Merging {running_interval} with {interval} for {event_name} {area} {location}"
                                )
                            else:
                                self.intervals[location][area][event_name].append(
                                    running_interval
                                )
                                running_interval = interval

                        self.intervals[location][area][event_name].append(
                            running_interval
                        )
                    else:
                        self.intervals[location][area][event_name] = []

                    if len(raw_intervals_without_downtime) > 0:
                        running_interval = raw_intervals_without_downtime[0]
                        for interval in raw_intervals_without_downtime[1:]:
                            if (
                                running_interval[1] >= interval[0]
                                and running_interval[1] < interval[1]
                            ):
                                running_interval[1] = interval[1]
                            else:
                                self.intervals_without_downtime[location][area][
                                    event_name
                                ].append(running_interval)
                                running_interval = interval
                        self.intervals_without_downtime[location][area][
                            event_name
                        ].append(running_interval)
                    else:
                        self.intervals_without_downtime[location][area][event_name] = []

                    print("---")
                    print(f"Done processing {event_name} {area} {location}.")
                    print(
                        "================================================================================\n"
                    )

    def _assemble_event_tuples(self):
        if self.intervals is None:
            self.compute_events()

        events = []

        for location_name, location_intervals in self.intervals.items():
            for area_name, area_intervals in location_intervals.items():
                for event_name, event_intervals in area_intervals.items():
                    sorted_event_intervals = sorted(
                        event_intervals, key=lambda interval: interval[0]
                    )
                    for interval in sorted_event_intervals:
                        event_start = datetime.datetime.strptime(
                            interval[0], "%Y-%m-%dT%H:%M:%S"
                        )
                        zone_name, start_tz_hrs, start_tz_mins = self._tz_hrs_mins(
                            location_name, event_start
                        )
                        event_start = event_start + datetime.timedelta(
                            hours=start_tz_hrs, minutes=start_tz_mins
                        )
                        event_end = datetime.datetime.strptime(
                            interval[1], "%Y-%m-%dT%H:%M:%S"
                        )
                        event_end = event_end + datetime.timedelta(
                            hours=start_tz_hrs, minutes=start_tz_mins
                        )
                        tz_str = " UTC+" if start_tz_hrs >= 0 else ""
                        tz_str += f"{start_tz_hrs:02d}:{start_tz_mins:02d}"
                        event_start_str = (
                            event_start.strftime("%Y-%m-%dT%H:%M:%S") + tz_str
                        )
                        event_end_str = event_end.strftime("%Y-%m-%dT%H:%M:%S") + tz_str
                        events.append(
                            [
                                "with_downtime",
                                location_name,
                                area_name,
                                event_name,
                                event_start_str,
                                event_end_str,
                            ]
                        )

        for (
            location_name,
            location_intervals,
        ) in self.intervals_without_downtime.items():
            for area_name, area_intervals in location_intervals.items():
                for event_name, event_intervals in area_intervals.items():
                    sorted_event_intervals = sorted(
                        event_intervals, key=lambda interval: interval[0]
                    )
                    for interval in sorted_event_intervals:
                        event_start = datetime.datetime.strptime(
                            interval[0], "%Y-%m-%dT%H:%M:%S"
                        )
                        zone_name, start_tz_hrs, start_tz_mins = self._tz_hrs_mins(
                            location_name, event_start
                        )
                        print(
                            "event_start, zone_name, start_hrs, start_min: ",
                            event_start,
                            zone_name,
                            start_tz_hrs,
                            start_tz_mins,
                        )
                        event_start = event_start + datetime.timedelta(
                            hours=start_tz_hrs, minutes=start_tz_mins
                        )
                        event_end = datetime.datetime.strptime(
                            interval[1], "%Y-%m-%dT%H:%M:%S"
                        )
                        zone_name, end_tz_hrs, end_tz_mins = self._tz_hrs_mins(
                            location_name, event_start
                        )
                        event_end = event_end + datetime.timedelta(
                            hours=end_tz_hrs, minutes=end_tz_mins
                        )
                        tz_str = " UTC+" if end_tz_hrs >= 0 else ""
                        tz_str += f"{end_tz_hrs:02d}:{end_tz_mins:02d}"
                        event_start_str = (
                            event_start.strftime("%Y-%m-%dT%H:%M:%S") + tz_str
                        )
                        event_end_str = event_end.strftime("%Y-%m-%dT%H:%M:%S") + tz_str
                        events.append(
                            [
                                "no_downtime",
                                location_name,
                                area_name,
                                event_name,
                                event_start_str,
                                event_end_str,
                            ]
                        )
        return events

    def save_events(self, parquet_file_name):
        print("save_events")
        events = self._assemble_event_tuples()
        df = pd.DataFrame(
            data=events,
            columns=[
                "type",
                "location_name",
                "area_name",
                "event_name",
                "start_datetime",
                "end_datetime",
            ],
        )
        df.to_parquet(parquet_file_name)

    def print_events(self):
        print("print_events")
        if self.intervals is None:
            self.compute_events()

        events = self._assemble_event_tuples()
        print("\n\n\nEvent list:\n")
        print(
            "Type            Location         Area        Event                      Intervals"
        )
        for (
            event_type,
            location_name,
            area_name,
            event_name,
            event_start_str,
            event_end_str,
        ) in events:
            print(
                f"{event_type:15s} {location_name:15s}  {area_name:10s}  {event_name:25s}  {event_start_str} - {event_end_str}"
            )

    def run(self, path):
        self.load_data(path)
        self.compute_derived_fields()
        self.compute_aggregations()
        self.compute_events()

    def run_incremental(self, location, path, file_name, overall_start=None):
        def pfx():
            return f"[{now()-overall_start:07.1f}] {location} - {file_name}:"

        if not overall_start:
            overall_start = now()
        agg_file_name = location + "_aggregates.json"
        if not os.path.isfile(agg_file_name):
            start = now()
            size = os.path.getsize("/".join([path, file_name])) / 1000.0
            print(f"{pfx()} Loading {size/1000.:.1f} KB data from {file_name} ...")
            self.load_data(path, data_file_name=file_name, location=location)
            n_rows = len(self.data[location])
            print(f"{pfx()} Loaded {n_rows:,d} rows within {now()-start:.1f} secs.")
            print(f"{pfx()} Computing derived fields for {location} ...")
            start = now()
            print(f"{pfx()} Computed derived fields within {now()-start:.1f} secs.")
            print(f"{pfx()} Computing aggregates for {location} ...")
            start = now()
            self.compute_aggregations(location)
            print(f"{pfx()} Computed aggregates within {now()-start:.1f} secs.")
            self.save_aggregate_data(agg_file_name, location)
            size = os.path.getsize(agg_file_name) / 1000.0
            print(
                f"{pfx()} Saved aggregatess for {location} to {agg_file_name} ({size:.1f}KB)."
            )
        else:
            print(f"{pfx()} Using precomputed aggregated.")
        start = now()
        self.load_aggregate_data(agg_file_name, location)
        print(f"{pfx()} Computed events within {now()-start:.1f} secs.")
        self.compute_events(location, log_prefix=pfx())
        events_file_name = location + "_events.json"
        self.save_events(events_file_name, location)
        print(f"{pfx()} Saved events for {location} to {events_file_name}.")
        for event_name, intervals in self.intervals[location].items():
            print(f'       - Event "{event_name}" had {len(intervals)} occurrences.')


if __name__ == "__main__":
    start = now()
    here = "."
    # yaml_file_name = "HH_vis_rain_all.yml"
    # yaml_file_name = "Berlin_vis_rain_snow.yml"
    yaml_file_name = sys.argv[1]
    # original: execute...py = einkommentiert
    pipeline = Pipeline(os.path.join(here, yaml_file_name))
    pipeline.load_data(here)
    pipeline.compute_aggregations()
    pipeline.save_aggregate_data(yaml_file_name.replace(".yml", ".parquet"))
    pipeline.compute_events()
    pipeline.save_events(yaml_file_name.replace(".yml", "_events.parquet"))
    pipeline.print_events()
    print(f"Completed pipeline within {now()-start:.1f} secs.")
