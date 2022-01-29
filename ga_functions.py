#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient

from config import *


def result_to_df(
    query=ga_ads_query, customer_id=ga_customer_id, fields=ga_ad_fields_list
):
    # Define local variables
    _next_page_token_str = "1"
    _i = 0
    _df = pd.DataFrame(columns=fields)

    # Initiate the Google API client
    _client = GoogleAdsClient.load_from_env()
    _ga_service = _client.get_service("GoogleAdsService")

    while _next_page_token_str != "":
        _search_request = _client.get_type("SearchGoogleAdsRequest")
        _search_request.customer_id = customer_id
        _search_request.query = query
        if _next_page_token_str != "1":
            _search_request.page_token = _next_page_token_str

        _result = _ga_service.search(_search_request)

        for _row in _result._response.results._pb:
            for _dimension in fields:
                _access_lst = _dimension.split(".")
                _value = _row
                for _ref in _access_lst:
                    _value = _value.__getattribute__(_ref)

                _df.loc[_i, _dimension] = _value
            _i += 1

        try:
            _next_page_token_str = _result._response._pb.next_page_token
        except Exception:
            _next_page_token_str = ""

        print(_next_page_token_str)

    return _df


def ga_to_df(
    ads_query=ga_ads_query,
    conversions_query=ga_conversions_query,
    columns_dict=ga_columns_dict,
):
    # Define local variables
    _source_dict = {2: "googlesearch", 6: "youtube", 3: "googledisplay"}

    # Populate the two dataframes we need -- ad performance and converions
    _ads_df = result_to_df(query=ads_query, fields=ga_ad_fields_list)
    _conversions_df = result_to_df(
        query=conversions_query, fields=ga_conversions_fields_list
    )

    # Isolate the conversions of interest and match them back into the ads
    _purchases_df = _conversions_df[
        _conversions_df["segments.conversion_action_name"] == "Purchase"
    ]
    _purchases_df = _purchases_df.rename(
        columns={
            "metrics.all_conversions": "purchases",
            "metrics.all_conversions_value": "revenue",
        }
    )
    _purchases_df = _purchases_df[
        ["segments.date", "ad_group_ad.ad.id", "purchases", "revenue"]
    ]

    _applications_df = _conversions_df[
        _conversions_df["segments.conversion_action_name"] == "Submitted Application"
    ]
    _applications_df = _applications_df.rename(
        columns={"metrics.all_conversions": "applications"}
    )
    _applications_df = _applications_df[
        ["segments.date", "ad_group_ad.ad.id", "applications"]
    ]

    _leads_df = _conversions_df[
        _conversions_df["segments.conversion_action_name"] == "Generate Lead"
    ]
    _leads_df = _leads_df.rename(columns={"metrics.all_conversions": "met_leads"})
    _leads_df = _leads_df[["segments.date", "ad_group_ad.ad.id", "met_leads"]]

    _ads_df = pd.merge(
        _ads_df, _purchases_df, how="left", on=["segments.date", "ad_group_ad.ad.id"]
    )
    _ads_df = pd.merge(
        _ads_df, _applications_df, how="left", on=["segments.date", "ad_group_ad.ad.id"]
    )
    _ads_df = pd.merge(
        _ads_df, _leads_df, how="left", on=["segments.date", "ad_group_ad.ad.id"]
    )

    # Clean up some column formatting and naming
    _ads_df["metrics.cost_micros"] = _ads_df["metrics.cost_micros"] / 1000000
    _ads_df = _ads_df.replace({"campaign.advertising_channel_type": _source_dict})
    _ads_df["ad_group_ad.ad.image_ad.name"] = np.where(
        _ads_df["ad_group_ad.ad.image_ad.name"] == "",
        _ads_df["ad_group_ad.ad.id"],
        _ads_df["ad_group_ad.ad.image_ad.name"],
    )
    _ads_df = _ads_df.rename(columns=columns_dict)
    _ads_df = _ads_df[[_x for _x in columns_dict.values()]]

    return _ads_df
