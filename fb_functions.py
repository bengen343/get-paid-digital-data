#!/usr/bin/env python
# coding: utf-8

import time

import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

from config import *


# Define a function to check on the status of the query and return it when it completes
def wait_for_async_job(
    app_id=fb_app_id,
    app_secret=fb_app_secret,
    access_token=fb_access_token,
    ad_account_id=fb_ad_account_id,
    fields_list=fb_fields_list,
    params=fb_params_dict,
):
    # Initiate Facebook API
    FacebookAdsApi.init(app_id, app_secret, access_token)

    # Create the job
    _job = AdAccount(ad_account_id).get_insights_async(
        fields=fields_list, params=params
    )

    # Monitor the jobs progress
    for _ in range(50):
        time.sleep(10)

        _job = _job.api_get()
        _status_str = _job.get("async_status")

        print(f"{_job.get('async_percent_completion')}% complete at {_ * 10} seconds")

        # Once the job completes pass the result cursor back up to the result
        if _status_str == "Job Completed":
            return _job.get_result(params={"limit": 1000})


# Define a function to turn the Facebook API results into a pretty dataframe
def fb_to_df(
    app_id=fb_app_id,
    app_secret=fb_app_secret,
    access_token=fb_access_token,
    ad_account_id=fb_ad_account_id,
    fields_list=fb_fields_list,
    params_dict=fb_params_dict,
    actions_list=fb_actions_list,
    columns_dict=fb_columns_dict,
):
    # Define local variables
    _columns_list = (
        fields_list[:-2] + actions_list + [x + "_value" for x in actions_list]
    )
    _columns_list.insert(0, "date")
    _df = pd.DataFrame(columns=_columns_list)
    _i = 0

    # Call the job via the Facebook API
    _result = wait_for_async_job(
        app_id=app_id,
        app_secret=app_secret,
        access_token=access_token,
        ad_account_id=ad_account_id,
        fields_list=fields_list,
        params=params_dict,
    )

    # Each result in the job represents one ad. Loop through them and add them to a pretty dataframe
    for _ad in _result:
        _df.loc[_i, "date"] = _ad.get("date_start")

        for _field in fields_list[:-2]:
            _df.loc[_i, _field] = _ad.get(_field)

        # The results for actions and their values are returned as dicts of those actions. Loop through them
        # independently and get the results for the actions we're interested in
        if _ad.get("actions") is not None:
            for _action in _ad.get("actions"):
                if _action.get("action_type") in actions_list:
                    _df.loc[_i, _action.get("action_type")] = float(
                        _action.get("value")
                    )

        if _ad.get("action_values") is not None:
            for _action in _ad.get("action_values"):
                if _action.get("action_type") in actions_list:
                    _df.loc[_i, _action.get("action_type") + "_value"] = float(
                        _action.get("value")
                    )

        _i += 1

    # Applications have messy. We need to find the right measure for them. This can be simplified in the future
    _df = _df.fillna(0)
    _df["met_applications"] = _df[
        [
            "add_to_cart",
            "offsite_conversion.fb_pixel_custom",
            "offsite_conversion.custom.3234878993405176",
        ]
    ].max(axis=1)
    _df["met_applications_value"] = _df[
        [
            "add_to_cart_value",
            "offsite_conversion.fb_pixel_custom_value",
            "offsite_conversion.custom.3234878993405176_value",
        ]
    ].max(axis=1)

    # Clean up the dataframe by unifying the column names, order, and deleting ads with no activity.
    _df["dim_source"] = "facebook"
    _df = _df.rename(columns=columns_dict)
    _df = _df[[_x for _x in columns_dict.values()]]
    for _column in [_x for _x in list(_df) if "met_" in _x]:
        _df[_column] = _df[_column].astype("float64")
    for _column in [_x for _x in list(_df) if "dim_" in _x]:
        _df[_column] = _df[_column].apply(lambda x: x.split(" ")[0])
    _df = _df[_df.iloc[:, 5:].sum(axis=1) != 0]

    return _df
