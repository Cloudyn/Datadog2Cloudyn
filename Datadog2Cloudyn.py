''' Getting all hosts CPU and save in Cloudyn CSV format '''

import time
import datetime
import csv
from datadog import initialize, api

def gethosts(query):
    '''return array of hosts by query'''
    result = api.Infrastructure.search(q=query)
    hosts = result["results"]["hosts"]
    return hosts

def getmetric(query):
    '''Get metric array by query'''
    now = int(time.time())
# we always ask for one hour rollup of samples, it might reault 2 samples per instance
    result = api.Metric.query(start=now - 3600, end=now, query=query)
    return result

def create_header_csv(fname):
    '''Create header for the CSV file'''
    with open(fname, 'a+b') as outfile:
        writer = csv.writer(outfile)
        # Write header
        writer.writerow(["Timestamp", "ResourceID", "Service", "Region", \
        "AvailabilityZone", "MetricName", "Unit", "Samples", "Average", \
        "Minimum", "Maximum", "OwnerID"])

def create_csv(fname):
    '''Write the metrics in the Cloudyn format'''
    itr = None
    with open(fname, 'a+b') as outfile:
        writer = csv.writer(outfile)
        for itr, item in enumerate(METRICS["Average_Metrics"]):
            # Convert date to this format 09-05-2017 7:00:00 AM
            target_date_time_ms = item[0]
            base_datetime = datetime.datetime(1970, 1, 1)
            delta = datetime.timedelta(0, 0, 0, target_date_time_ms)
            target_date = base_datetime + delta
            # Write the samples to CSV
        writer.writerow([
            target_date,
            METRICS["HostID"],#ResourceID
            METRICS["Service"],#Service
            METRICS["Region"],#Region
            METRICS["Availability_Zone"], #AZ
            METRICS["Metric_Name"], #MetricName
            METRICS["Unit"], #Unit
            METRICS["NumberOfSamples"], #Samples
            METRICS["Average_Metrics"][itr][1],#Avg
            METRICS["Minimum_Metrics"][itr][1],#Min
            METRICS["Maximum_Metrics"][itr][1],#Max
            METRICS["AWS_Account"] # OwnerID
            ])

OPTIONS = {
    #Your API_KEY and APP_KEY, see https://app.datadoghq.com/account/settings#api
    'api_key':'api_key',
    'app_key':'app_key'
    }

initialize(**OPTIONS)

    #see your list here: https://app.datadoghq.com/metric/summary.
    #Select the host from which you want the data,
    #see here: https://app.datadoghq.com/infrastructure

METRICS = {}
AZONE = ""
REGION = ""
AWS_ACCOUNT = ""
METRIC_NAME = "aws.ec2.cpuutilization"
TIMESTAMP = str(int(time.time()))
HOSTS_ARR = gethosts("hosts:")
for idx, host in enumerate(HOSTS_ARR):
    host_tags = api.Tag.get(host)
    for tag in host_tags["tags"]:
        if "region" in tag:
            REGION = tag.split(":", 1)[1]
        if "aws_account" in tag:
            AWS_ACCOUNT = tag.split(":", 1)[1]
        if "availability-zone" in tag:
            AZONE = tag.split(":", 1)[1]
    METRICS["Region"] = REGION
    METRICS["AWS_Account"] = AWS_ACCOUNT
    METRICS["Availability_Zone"] = AZONE
    METRICS["HostID"] = host
    METRICS["Metric_Name"] = "CPUUtilization"
    METRICS["Service"] = "EC2"
    METRICS["NumberOfSamples"] = "60"
    cpu_avg_query = "avg:"+ METRIC_NAME +"{host:" + host + "}.rollup(avg, 3600)"
    cpu_max_query = "max:"+ METRIC_NAME +"{host:" + host + "}.rollup(max, 3600)"
    cpu_min_query = "min:"+ METRIC_NAME +"{host:" + host + "}.rollup(min, 3600)"
    avg_metric_array = getmetric(cpu_avg_query)
    max_metric_array = getmetric(cpu_max_query)
    min_metric_array = getmetric(cpu_min_query)
    filename = AWS_ACCOUNT + "_" + METRIC_NAME + "_" + TIMESTAMP + ".csv"
    if idx == 0:
        create_header_csv(filename)
    if len(avg_metric_array["series"]) > 0:
        METRICS['Average_Metrics'] = avg_metric_array['series'][0]["pointlist"]
        METRICS['Maximum_Metrics'] = max_metric_array['series'][0]["pointlist"]
        METRICS['Minimum_Metrics'] = min_metric_array['series'][0]["pointlist"]
        METRICS["Unit"] = avg_metric_array["series"][0]["unit"][0]["name"]
        create_csv(filename)
