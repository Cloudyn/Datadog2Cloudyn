# Datadog2Cloudyn - Send CPU metrics sample

Datadog Metric Collection Sample demonstrates integrating Datadog with Cloudyn for presenting metrics collected by Datadog in Cloudyn reports.
In this sample we collect CPU utilization (Avg, Max, Min)
As with any code, please test before using in production.

## Table of contents

* [datadog-to-cloudyn.py](datadog-to-cloudyn.py)

## Installation
Install from pip:

    pip install datadog

For more information see https://github.com/DataDog/datadogpy

Please note, API_KEY and APP_KEY are input parameters, use your API_KEY and APP_KEY
```python
	api_key = sys.argv[1]
    	app_key = sys.argv[2]
    	OPTIONS["api_key"] = api_key
    	OPTIONS["app_key"] = app_key
```
