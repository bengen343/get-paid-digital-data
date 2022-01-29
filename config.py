import json
import os
import tempfile
from datetime import datetime, timedelta, date

from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

# Date variables
yesterday_dt = datetime.today().date() - timedelta(days=1)
days_14_dt = datetime.today().date() - timedelta(days=14)
days_90_dt = datetime.today().date() - timedelta(days=94)
months_3_dt = date(days_90_dt.year, days_90_dt.month, 1)

yesterday_str = yesterday_dt.strftime("%Y-%m-%d")
days_14_str = days_14_dt.strftime("%Y-%m-%d")
days_90_str = days_90_dt.strftime("%Y-%m-%d")
months_3_str = months_3_dt.strftime("%Y-%m-%d")

# ga_functions variables
# Google Ads API Auth variables
ga_customer_id = os.environ.get("GA_CUSTOMER_ID")

# Google Ads API query variables
ga_ad_fields_list = [
    "segments.date",
    "campaign.name",
    "campaign.advertising_channel_type",
    "ad_group.name",
    "ad_group_ad.ad.image_ad.name",
    "ad_group_ad.ad.id",
    "metrics.cost_micros",
    "metrics.clicks",
    "metrics.impressions",
]

ga_conversions_fields_list = [
    "segments.date",
    "segments.conversion_action_name",
    "ad_group_ad.ad.id",
    "metrics.all_conversions",
    "metrics.all_conversions_value",
]

ga_columns_dict = {
    "segments.date": "dim_date",
    "campaign.advertising_channel_type": "dim_source",
    "campaign.name": "dim_campaign",
    "ad_group.name": "dim_ad_group",
    "ad_group_ad.ad.image_ad.name": "dim_ad",
    "metrics.cost_micros": "met_spent",
    "metrics.impressions": "met_impressions",
    "metrics.clicks": "met_clicks",
    "leads": "met_leads",
    "applications": "met_applications",
    "purchases": "met_purchases",
    "revenue": "met_purchase_value",
}

ga_ads_query = (
    """
    SELECT
        segments.date,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        campaign.advertising_channel_type,
        ad_group_ad.ad.id,
        ad_group_ad.ad.name,
        ad_group_ad.ad.image_ad.name,
        metrics.cost_micros,
        metrics.clicks,
        metrics.impressions
    FROM ad_group_ad
    WHERE segments.date BETWEEN '"""
    + months_3_str
    + """' AND '"""
    + yesterday_str
    + """'
    ORDER BY segments.date ASC
    """
)

ga_conversions_query = (
    """
    SELECT
        segments.date,
        segments.conversion_action_name,
        ad_group_ad.ad.id,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.view_through_conversions
    FROM ad_group_ad
    WHERE
        segments.date BETWEEN '"""
    + months_3_str
    + """' AND '"""
    + yesterday_str
    + """' 
        AND segments.conversion_action_name IN ('Submitted Application', 'Purchase', 'Generate Lead')
    ORDER BY segments.date ASC
    """
)

# fb_functions variables
# Facebook API Auth variables
fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
fb_ad_account_id = os.environ.get("FB_ACCOUNT_ID")
fb_app_secret = os.environ.get("FB_APP_SECRET")
fb_app_id = os.environ.get("FB_APP_ID")

# Facebook reporting parameters and fields
fb_params_dict = {
    "time_range": {"since": months_3_str, "until": yesterday_str},
    "time_increment": 1,
    "level": "ad",
}

fb_fields_list = [
    "campaign_name",
    "adset_name",
    "ad_name",
    "spend",
    "impressions",
    "reach",
    "clicks",
    "actions",
    "action_values",
]

fb_actions_list = [
    "omni_view_content",
    "lead",
    "add_to_cart",
    "initiate_checkout",
    "omni_purchase",
    "offsite_conversion.fb_pixel_custom",
    "offsite_conversion.custom.3234878993405176",
]

fb_columns_dict = {
    "date": "dim_date",
    "source": "dim_source",
    "campaign_name": "dim_campaign",
    "adset_name": "dim_ad_group",
    "ad_name": "dim_ad",
    "spend": "met_spent",
    "reach": "met_reach",
    "impressions": "met_impressions",
    "clicks": "met_clicks",
    "omni_view_content": "met_view_content",
    "lead": "met_leads",
    "applications": "met_applications",
    "add_to_cart": "met_add_to_cart",
    "initiate_checkout": "met_initiate_checkout",
    "omni_purchase": "met_purchases",
    "omni_view_content_value": "met_view_content_value",
    "lead_value": "met_leads_value",
    "applications_value": "met_applications_value",
    "add_to_cart_value": "met_add_to_cart_value",
    "initiate_checkout_value": "met_initiate_checkout_value",
    "omni_purchase_value": "met_purchase_value",
}

fb_columns_list = (
    fb_fields_list[:-2] + fb_actions_list + [x + "_value" for x in fb_actions_list]
)
fb_columns_list.insert(0, "date")

# Establish BigQuery credentials
bq_account_creds = json.loads(os.environ.get("BQ_ACCOUNT_CREDS"))
bq_credentials = service_account.Credentials.from_service_account_info(bq_account_creds)

bq_project_id = "paid-digital-data-blend"
bq_table_id = (
    "paid-digital-data-blend.paid_digital_performance.paid_digital_performance_all"
)

bq_schema_list = [
    {"name": "dim_date", "type": "DATETIME", "mode": "REQUIRED"},
    {"name": "dim_source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "dim_campaign", "type": "STRING", "mode": "REQUIRED"},
    {"name": "dim_ad_group", "type": "STRING", "mode": "REQUIRED"},
    {"name": "dim_ad", "type": "STRING", "mode": "REQUIRED"},
    {"name": "dim_objective", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dim_productline", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dim_product", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dim_program", "type": "STRING", "mode": "NULLABLE"},
    {"name": "met_spent", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "met_reach", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_impressions", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_clicks", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_view_content", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_leads", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_applications", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_add_to_cart", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_initiate_checkout", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_purchases", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_view_content_value", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_leads_value", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_applications_value", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_add_to_cart_value", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_initiate_checkout_value", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "met_purchase_value", "type": "FLOAT64", "mode": "NULLABLE"},
]

# Google sheet information
tracker_url = "https://docs.google.com/spreadsheets/d/1seQLgtCn7mJgpQxEtRa8uKsft3CwoI-qJ0HiG9hrny8/edit#gid=1504545389"
tracker_gs_key = "1seQLgtCn7mJgpQxEtRa8uKsft3CwoI-qJ0HiG9hrny8"
social_weekly_wks = "Paid Social - Weekly"
social_weekly_wks_id = "39281733"
social_monthly_wks = "Paid Social - Monthly"
social_monthly_wks_id = "600837986"
creatives_gs_key = "1oL1qkGOvDI_fPfJcpRzW4ge_dQ7wdRRTEGkoa4lkXI4"
creatives_gs_wks = "Creatives"


# Establish Commercial Tracker Google Sheet credentials
def _google_creds_as_file():
    _temp = tempfile.NamedTemporaryFile(dir=".", delete=False)
    _temp.write(
        json.dumps(
            {
                "type": os.environ.get("GOOGLE_ACCOUNT_TYPE"),
                "project_id": os.environ.get("GOOGLE_PROJECT_ID"),
                "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID"),
                "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace(
                    "\\n", "\n"
                ),
                "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
                "client_id": os.environ.get("GOOGLE_CLIENT_ID"),
                "auth_uri": os.environ.get("GOOGLE_AUTH_URI"),
                "token_uri": os.environ.get("GOOGLE_TOKEN_URI"),
                "auth_provider_x509_cert_url": os.environ.get("GOOGLE_PROVIDER_URL"),
                "client_x509_cert_url": os.environ.get("GOOGLE_CERT_URL"),
            }
        ).encode("utf-8")
    )
    _temp.close()
    return _temp


gs_credentials = _google_creds_as_file()

# Establish RY Creative directory Google Sheet credentials
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
API_NAME = "sheets"
API_VERSION = "v4"

cd_account_creds = json.loads(os.environ.get("GS_ACCOUNT_CREDS"))
cd_credentials = ServiceAccountCredentials.from_json_keyfile_dict(cd_account_creds)

# LinkedIn variables
linkedin_dict = {
    "Start Date (in UTC)": "dim_date",
    "source": "dim_source",
    "Campaign Name": "dim_campaign",
    "DSC Name": "dim_ad_group",
    "Creative Name": "dim_ad",
    "Total Spent": "met_spent",
    "Reach": "met_reach",
    "Impressions": "met_impressions",
    "Clicks": "met_clicks",
    "Leads": "met_leads",
    "applications": "met_applications",
    "purchases": "met_purchases",
    "applications_value": "met_applications_value",
    "purchases_value": "met_purchase_value",
}

# Create a list of columns that need to be converted to floats at the end
creatives_dict = {
    "Objective": "dim_objective",
    "Productline": "dim_productline",
    "Product": "dim_product",
    "Program": "dim_program",
}

agg_dict = {
    "met_impressions": "sum",
    "met_clicks": "sum",
    "met_leads": "sum",
    "met_applications": "sum",
    "met_purchases": "sum",
    "met_purchase_value": "sum",
    "met_spent": "sum",
}
