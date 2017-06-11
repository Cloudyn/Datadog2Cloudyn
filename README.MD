# Datadog2Cloudyn - Send CPU metrics sample

Datadog Metric Collection Sample demonstrates integrating Datadog with Cloudyn for presenting metrics collected by Datadog in Cloudyn reports.
In this sample we collect CPU utilization (Avg, Max, Min)
As with any code, please test before using in production.

## Table of contents

* [Datadog2Cloudyn](Datadog2Cloudyn.py)

## Installation
Install from pip:

    pip install datadog

For more information see https://github.com/DataDog/datadogpy

Make sure to update:
```python
	options = {
		'api_key':'api_key',
		'app_key':'app_key'
	} 
```

to your API_KEY and APP_KEY
