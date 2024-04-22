import argparse
import os
from datetime import datetime
from time import sleep

import base64
import boto3
import pandas as pd
import pyspark.sql.functions as f
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

config_yaml = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'local_market.yaml')))


def get_cities(base64string, month):
    body = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
             <soap:Body>
                 <DiscoverParameterValues xmlns="http://comscore.com/">
                 <parameterId>csMarket</parameterId>
                 <query xmlns="http://comscore.com/ReportQuery">
                     <Parameter KeyId="dataSource" Value="1" />
                     <Parameter KeyId="geo" Value="840" />
                     <Parameter KeyId="timeType" Value="1" />
                     <Parameter KeyId="timePeriod" Value="{month}"/>
                 </query>
                 </DiscoverParameterValues>
             </soap:Body>
            </soap:Envelope>""".format(month=month)

    encoded_request = body.encode('utf-8')

    headers = {"POST": "/KeyMeasuresDMA.asmx",
               "Content-Type": "text/xml; charset=UTF-8",
               "Content-Length": str(len(encoded_request)),
               "SOAPAction": "http://comscore.com/DiscoverParameterValues",
               "Authorization": "Basic %s" % base64string}

    url = "https://api.comscore.com/KeyMeasures.asmx?WSDL"

    response = requests.post(url, data=encoded_request, headers=headers)

    return BeautifulSoup(response.content, "lxml").findAll('enumvalue')


def get_report_id(city, month, time_type, base64string, media_set, media_set_type):
    body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
    xmlns:com="http://comscore.com/" xmlns:rep="http://comscore.com/ReportQuery">
       <soapenv:Header/>
       <soapenv:Body>
          <com:SubmitReport>
             <rep:query>
                <rep:Parameter KeyId="dataSource" Value="1"/>
                <rep:Parameter KeyId="csMarket" Value="{city}"/>
                <rep:Parameter KeyId="geo" Value="840"/>
                <rep:Parameter KeyId="timeType" Value="{time_type}"/>
                <rep:Parameter KeyId="timePeriod" Value="{month}"/>
                <rep:Parameter KeyId="mediaSet" Value="{mediaset}"/>
                <rep:Parameter KeyId="mediaSetType" Value="{mediasettype}"/>
                <rep:Parameter KeyId="loc" Value="0"/>
                <rep:Parameter KeyId="measure" Value="1" />
                <rep:Parameter KeyId="measure" Value="9" />
                <rep:Parameter KeyId="measure" Value="10" />
                <rep:Parameter KeyId="measure" Value="70" />
                <rep:Parameter KeyId="measure" Value="71" />
                <rep:Parameter KeyId="measure" Value="7" />
                <rep:Parameter KeyId="measure" Value="2" />
                <rep:Parameter KeyId="measure" Value="14" />
                <rep:Parameter KeyId="measure" Value="3" />
                <rep:Parameter KeyId="measure" Value="16" />
                <rep:Parameter KeyId="measure" Value="15" />
                <rep:Parameter KeyId="measure" Value="8" />
                <rep:Parameter KeyId="measure" Value="6" />
                <rep:Parameter KeyId="measure" Value="5" />
                <rep:Parameter KeyId="measure" Value="11" />
                <rep:Parameter KeyId="measure" Value="12" />
                <rep:Parameter KeyId="measure" Value="143" />
                <rep:Parameter KeyId="measure" Value="144" />
                <rep:Parameter KeyId="measure" Value="145" />
                <rep:Parameter KeyId="measure" Value="146" />
                <rep:Parameter KeyId="measure" Value="274" />
                <rep:Parameter KeyId="targetType" Value="0"/>
                <rep:Parameter KeyId="targetGroup" Value="15"/>
             </rep:query>
          </com:SubmitReport>
       </soapenv:Body>
    </soapenv:Envelope>""".format(city=city, time_type=time_type, month=month, mediaset=media_set,
                                  mediasettype=media_set_type)

    encoded_request = body.encode('utf-8')
    headers = {"POST": "/KeyMeasures.asmx",
               "Content-Type": "text/xml; charset=UTF-8",
               "Content-Length": str(len(encoded_request)),
               "SOAPAction": "http://comscore.com/SubmitReport",
               "Authorization": "Basic %s" % base64string}

    url = "https://api.comscore.com/KeyMeasures.asmx?WSDL"

    response = requests.post(url, data=encoded_request, headers=headers)
    logger.info("response.content ::::: {}".format(response.content))
    bs = BeautifulSoup(response.content, features='lxml')
    job_id = bs.find('jobid').text.strip()

    return job_id


def get_data(city, city_name, month, time_type, base64string, media_set, media_set_type):
    raw_data = []
    processed_data = []
    job_id = get_report_id(city, month, time_type, base64string, media_set, media_set_type)

    # Use the job ID to fetch report https://api.comscore.com/DMA/KeyMeasuresDMA.asmx?op=FetchReport
    body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:com="http://comscore.com/">
       <soapenv:Header/>
       <soapenv:Body>
          <com:FetchReport>
             <!--type: int-->
             <com:jobId>{job_id}</com:jobId>
          </com:FetchReport>
       </soapenv:Body>
    </soapenv:Envelope>""".format(job_id=job_id)

    encoded_request = body.encode('utf-8')

    headers = {"POST": "/KeyMeasuresDMA.asmx",
               "Content-Type": "text/xml; charset=UTF-8",
               "Content-Length": str(len(encoded_request)),
               "SOAPAction": "http://comscore.com/FetchReport",
               "Authorization": "Basic %s" % base64string}

    url = "https://api.comscore.com/KeyMeasures.asmx?WSDL"

    response = requests.post(url, data=encoded_request, headers=headers)

    bs = BeautifulSoup(response.content, features='lxml')

    while bs.find('errors'):
        #  Some reports will return
        #  Report request corresponding to this job id is queued and will be processed shortly. Please try again.
        sleep(10)
        response = requests.post(url, data=encoded_request, headers=headers)
        bs = BeautifulSoup(response.content, features='lxml')

    table_body = bs.find('table')
    rows = table_body.find_all('tr')

    for row in rows[1:]:
        cols = row.find_all('td')
        index = row.find('th')
        extra = []
        extra += [index['media_type'].replace("[", "").replace("]", "")] if index.has_attr('media_type') else [None]
        extra += [index['web_id']] if index.has_attr('web_id') else [None]
        extra += [index['parent_id']] if index.has_attr('parent_id') else [None]
        cols = [index.text.strip()] + extra + [str(x.text.strip()) for x in cols]
        raw_data += [cols]

    # Get the list of metrics names
    header = rows[0].find_all('td')
    header = [str(rows[0].find('th').text.strip().lower()), 'media_type', 'web_id', 'parent_id'] + header

    # Loop through the metrics data to put one metric and one media as one row
    for row in raw_data:
        for i in range(4, len(raw_data[0])):
            if i >= len(row):
                processed_data += [row[:4] + [header[i].text.strip(), header[i]['id'], 'None']]
            else:
                processed_data += [row[:4] + [header[i].text.strip(), header[i]['id'], row[i]]]

    df = pd.DataFrame(processed_data)
    df.columns = ['media', 'media_type', 'company_id', 'parent_company_id', 'metric', 'metric_id', 'metric_value']
    df['region'] = city_name
    df['state'] = city_name[-2:]
    df['region_id'] = city
    df['metric_value'] = pd.to_numeric(df["metric_value"], errors='coerce')

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse arguments passed')
    parser.add_argument('--role-arn', help='Role ARN to be used when reading & writing to S3.', required=True)
    parser.add_argument('--month', help='the execution day')
    parser.add_argument('--output-path', help='Output path')
    parser.add_argument('--table', help='table name')
    parser.add_argument('--api-call', help='api user name and password')
    parser.add_argument('--start-date', help='the date of the first available report')
    parser.add_argument('--start-month-id', help='the ID for the first report')
    parser.add_argument("--secrets-role", help="Secrets Role")

    args = parser.parse_args()
    output_path = args.output_path
    month = args.month[:-2] + '01'  # change the date to the first day 2019-01-01
    table = args.table
    api_call = args.api_call
    start_date = args.start_date
    start_month_id = args.start_month_id

    diff = relativedelta.relativedelta(datetime.strptime(month, '%Y-%m-%d'), datetime.strptime(start_date, '%Y-%m-%d'))
    diff_months = diff.months
    # add in the number of months (12) for difference in years
    diff_months += 12 * diff.years
    month_id = diff_months + int(start_month_id)

    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

    # Assume AWS role
    spark._jsc.hadoopConfiguration().set('amz-assume-role-arn', args.role_arn)
    spark.sql("set spark.hadoop.fs.s3.maxRetries=20")
    spark.sql("set parquet.compression=snappy")

    time_frame_list = config_yaml['timeframe']
    categories = config_yaml['categories']

    session = boto3.Session(region_name='us-west-2')
    sts_client = session.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=args.secrets_role, RoleSessionName="Boto3Session")
    credentials = assumed_role_object['Credentials']
    ssm_session = boto3.session.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )
    ssm_session_client = ssm_session.client('ssm')
    token_json = ssm_session_client.get_parameter(Name=api_call, WithDecryption=True)["Parameter"]["Value"]
    username, password = token_json.split(',')
    base64string = base64.encodestring(('%s:%s' % (username, password)).encode()).decode().strip()
    cities = get_cities(base64string, month_id)

    data = []
    for city in cities:
        for time_frame in time_frame_list:
            for category in categories:
                df = get_data(city['id'], city['value'], month_id, time_frame['id'], base64string,
                              category['media_set'], category['media_set_type'])
                df['timeframe'] = time_frame['time']
                df['month'] = datetime.strptime(month, '%Y-%m-%d').strftime('%B')
                df['year'] = month[:4]
                df['year'] = pd.to_numeric(df["year"])
                df['report_type'] = 'Local Market Single Region'
                df['categories'] = category['name']
                data.append(df)
    result = pd.concat(data, ignore_index=True)
    schema = StructType([StructField("media", StringType(), nullable=True),
                         StructField("media_type", StringType(), nullable=True),
                         StructField("company_id", StringType(), nullable=True),
                         StructField("parent_company_id", StringType(), nullable=True),
                         StructField("metric", StringType(), nullable=True),
                         StructField("metric_id", StringType(), nullable=True),
                         StructField("metric_value", FloatType(), nullable=True),
                         StructField("region", StringType(), nullable=True),
                         StructField("state", StringType(), nullable=True),
                         StructField("region_id", StringType(), nullable=True),
                         StructField("timeframe", StringType(), nullable=True),
                         StructField("month", StringType(), nullable=True),
                         StructField("year", IntegerType(), nullable=True),
                         StructField("report_type", StringType(), nullable=True),
                         StructField("categories", StringType(), nullable=True)])

    df = spark.createDataFrame(result, schema=schema)

    # Changed NaN to Null
    columns = df.columns
    for column in columns:
        df = df.withColumn(column, f.when(f.isnan(f.col(column)), None).otherwise(f.col(column)))
    destination_path = 's3://' + output_path + '/zillow/comscore/{table}/{month}/'.format(table=table, month=month)
    df.write.mode('overwrite').format('parquet').save(destination_path)

    sql_string = """
        ALTER TABLE comscore.{table_name} ADD IF NOT EXISTS PARTITION (data_date='{partition}') LOCATION '{path}'
        """.format(table_name=table, partition=month, path=destination_path)

    spark.sql(sql_string)

    spark.stop()
