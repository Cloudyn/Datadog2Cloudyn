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
        for dic in Metric_List:
            for itr in range(0, len(dic["Average_Metrics"])):
            #for itr, met in enumerate(dic["Average_Metrics"]):
                # Convert date to this format 09-05-2017 7:00:00 AM
                target_date_time_ms = dic["Average_Metrics"][itr][0]
                base_datetime = datetime.datetime(1970, 1, 1)
                delta = datetime.timedelta(0, 0, 0, target_date_time_ms)
                target_date = base_datetime + delta
                # Write the samples to CSV
                writer.writerow([
                    target_date,
                    dic["HostID"],#ResourceID
                    dic["Service"],#Service
                    dic["Region"],#Region
                    dic["Availability_Zone"], #AZ
                    dic["Metric_Name"], #MetricName
                    dic["Unit"], #Unit
                    dic["NumberOfSamples"], #Samples
                    dic["Average_Metrics"][itr][1],#Avg
                    dic["Minimum_Metrics"][itr][1],#Min
                    dic["Maximum_Metrics"][itr][1],#Max
                    dic["AWS_Account"] # OwnerID
                    ])

OPTIONS = {
    #Your API_KEY and APP_KEY, see https://app.datadoghq.com/account/settings#api
    'api_key': '49591a0ddb33a31205c651acaf5bbe77',
    'app_key': '57ac4012b4443748380fd9caddcc098e7a1e7008'
    }

initialize(**OPTIONS)

    #see your list here: https://app.datadoghq.com/metric/summary.
    #Select the host from which you want the data,
    #see here: https://app.datadoghq.com/infrastructure
Metric_List = []
METRIC_NAME = "aws.ec2.cpuutilization"
TIMESTAMP = str(int(time.time()))
filename = METRIC_NAME + "_" + TIMESTAMP + ".csv"
HOSTS_ARR = gethosts("hosts:")
for idx, host in enumerate(HOSTS_ARR):
    METRICS = {}
    AZONE = ""
    REGION = ""
    # default aws account, if account is a tag called aws_account we will use it instead
    AWS_ACCOUNT = "287063015643"
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
    metric_array = getmetric(cpu_avg_query + "," + cpu_max_query + "," + cpu_min_query)
    if idx == 0:
        create_header_csv(filename)
    if len(metric_array["series"]) > 2:
        METRICS['Average_Metrics'] = metric_array['series'][0]["pointlist"]
        METRICS['Maximum_Metrics'] = metric_array['series'][1]["pointlist"]
        METRICS['Minimum_Metrics'] = metric_array['series'][2]["pointlist"]
        METRICS["Unit"] = metric_array["series"][0]["unit"][0]["name"]
        Metric_List.append(METRICS)
create_csv(filename)
