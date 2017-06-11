''' Getting all hosts CPU and save in Cloudyn CSV format '''

import time
import datetime
import csv
from datadog import initialize, api
import json
import sys

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

def create_csv(fname, metrics):
    '''Write the metrics in the Cloudyn format'''
    if not metrics:
        return
    with open(fname, 'a+b') as outfile:
        writer = csv.writer(outfile)

        writer.writerow(["Timestamp", "ResourceID", "Service", "Region", \
        "AvailabilityZone", "MetricName", "Unit", "Samples", "Average", \
        "Minimum", "Maximum", "OwnerID"])

        for dic in metrics:
            for itr in range(0, len(dic["Average_Metrics"])):
            #for itr, met in enumerate(dic["Average_Metrics"]):
                # Convert date to this format 09-05-2017 7:00:00 AM
                target_date_time_ms = int(dic["Average_Metrics"][itr][0])/1000
                target_date = datetime.datetime.utcfromtimestamp(target_date_time_ms)
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
    'api_key': '',
    'app_key': ''
    }


if __name__ == "__main__":
    api_key = sys.argv[1]
    app_key = sys.argv[2]
    OPTIONS["api_key"] = api_key
    OPTIONS["app_key"] = app_key

    print "Initializing"
    initialize(**OPTIONS)

    #see your list here: https://app.datadoghq.com/metric/summary.
    #Select the host from which you want the data,
    #see here: https://app.datadoghq.com/infrastructure

    Metric_List = []
    METRIC_NAME = "aws.ec2.cpuutilization"
    TIMESTAMP = str(int(time.time()))
    filename = METRIC_NAME + "_" + TIMESTAMP + ".csv"
    print "Getting hosts"
    HOSTS_ARR = gethosts("hosts:")
    print "Hosts: %s" % HOSTS_ARR

    for host in HOSTS_ARR:
        print "Processing host %s" % host
        METRICS = {}
        AZONE = ""
        REGION = ""
        # default aws account, if account is a tag called aws_account we will use it instead
        AWS_ACCOUNT = "287063015643"
        cpu_avg_query = "avg:"+ METRIC_NAME +"{host:" + host + "}.rollup(avg, 3600)"
        cpu_max_query = "max:"+ METRIC_NAME +"{host:" + host + "}.rollup(max, 3600)"
        cpu_min_query = "min:"+ METRIC_NAME +"{host:" + host + "}.rollup(min, 3600)"
        print "Getting metrics"
        metric_array = getmetric(cpu_avg_query + "," + cpu_max_query + "," + cpu_min_query)

        if len(metric_array["series"]) > 2:
            print "Getting tags"
            host_tags = api.Tag.get(host)
            print "Tags: %s" % host_tags
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

            METRICS['Average_Metrics'] = metric_array['series'][0]["pointlist"]
            METRICS['Maximum_Metrics'] = metric_array['series'][1]["pointlist"]
            METRICS['Minimum_Metrics'] = metric_array['series'][2]["pointlist"]
            METRICS["Unit"] = metric_array["series"][0]["unit"][0]["name"]
            Metric_List.append(METRICS)
        else:
            print "Not enough results, skipping them"

    print "Writing all metrics to %s" % filename
    create_csv(filename, Metric_List)
