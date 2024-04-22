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
from string import Template

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# This report is to extract Desktop Only data source from https://api.comscore.com/CrossVisit.asmx?wsdl
# including one month average report

config_yaml = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cross_visiting.yaml')))


def get_processed_df(processed_data):
    # Create dataframe
    df = pd.DataFrame(processed_data)
    df.columns = ['benchmark_media', 'benchmark_media_id', 'media_target_audience',
                  'metric', 'metric_id', 'metric_value', 'comparison_media', 'target']
    df['metric_value'] = pd.to_numeric(df["metric_value"], errors='coerce')
    
    # reshap df by pivot function as per bussiness logic 
    df = df.pivot(index=['benchmark_media', 'benchmark_media_id', 'target', 'comparison_media', 'media_target_audience'], columns=['metric'], values=['metric_value']).reset_index()

    return df


def generate_parameters(month, time_type, target_type, measures, reports):
    """
    :param month: month_id is time period.
    :param time_type: average time type eg: 1 Month Average.
    :param target_type: Target type is dictionary of target key and target value.
    :param measures: Measure is dictionary of metric id and its name.
    :param report: Report is dictionary of media id and its name.
    """
    xml = []
    xml += ['<rep:Parameter KeyId="geo" Value="840"/>']
    xml += ['<rep:Parameter KeyId="targetType" Value="0"/>']
    xml += ['<rep:Parameter KeyId="mediaRowType" Value="0"/>']
    xml += ['<rep:Parameter KeyId="timeType" Value="{id}"/>'.format(id=time_type)]
    xml += ['<rep:Parameter KeyId="timePeriod" Value="{month}"/>'.format(month=month)]
    for media in reports:
        xml += ['<rep:Parameter KeyId="mediaColumn" Value="{id}"/>'.format(id=media['id'])]
    for measure in measures:
        xml += ['<rep:Parameter KeyId="measure" Value="{id}"/>'.format(id=measure['id'])]
    for target_key in target_type:
        xml += ['<rep:Parameter KeyId="{key}" Value="{value}"/>'.format(key=target_key, value=target_type[target_key])]

    return xml


def get_report_id(month, time_type, base64string, target_type, measures, reports):
    """
    Function will write job_id for the given xml script which is further used to fetch the cross_visiting data
    :param month: month_id is time period.
    :param time_type: average time type eg: 1 Month Average.
    :param base64string: binary-to-text encoding.
    :param target_type: Target type is dictionary of target key and target value.
    :param measures: Measure is dictionary of metric id and its name.
    :return: job_id
    """
    xml = generate_parameters(month, time_type, target_type, measures, reports)

    body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                xmlns:com="http://comscore.com/" xmlns:rep="http://comscore.com/ReportQuery">
                <soapenv:Header/>
                    <soapenv:Body>
                        <com:SubmitReport>
                            <rep:query>
                                {xml}
                            </rep:query>
                        </com:SubmitReport>
                    </soapenv:Body>
                </soapenv:Envelope>""".format(xml=''.join(map(str, xml)))
    print(body)
    encoded_request = body.encode('utf-8')
    headers = {"POST": "/CrossVisit.asmx",
               "Content-Type": "text/xml; charset=UTF-8",
               "Content-Length": str(len(encoded_request)),
               "SOAPAction": "http://comscore.com/SubmitReport",
               "Authorization": "Basic %s" % base64string}

    url = "https://api.comscore.com/CrossVisit.asmx?wsdl"
    response = requests.post(url, data=encoded_request, headers=headers)
    logger.info("response.content ::::: {}".format(response.content))
    bs = BeautifulSoup(response.content, features='lxml')
    job_id = bs.find('jobid').text.strip()

    return job_id


def get_data(month, time_type, base64string, target_type, measures, reports):
    """
    Function will fetch the crossvisiting data report through the job id with xml script
    :param month: month_id is time period.
    :param time_type: average time type eg: 1 Month Average.
    :param base64string: binary-to-text encoding.
    :param target_type: Target type is dictionary of target key and target value.
    :param measures: Measure is dictionary of metric id and its name.
    :return: pandas dataframe
    """
    raw_data = []
    processed_data = []

    job_id = get_report_id(month, time_type, base64string, target_type, measures, reports)
    # Use the job ID to fetch report https://api.comscore.com/CrossVisit.asmx?wsdl?op=FetchReport
    
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
    headers = {"POST": "/CrossVisit.asmx",
               "Content-Type": "text/xml; charset=UTF-8",
               "Content-Length": str(len(encoded_request)),
               "SOAPAction": "http://comscore.com/FetchReport",
               "Authorization": "Basic %s" % base64string}
    url = "https://api.comscore.com/CrossVisit.asmx?wsdl"
    response = requests.post(url, data=encoded_request, headers=headers)
    bs = BeautifulSoup(response.content, features='lxml')

    while bs.find('errors'):
        #  Some reports will return
        #  Report request corresponding to this job id is queued and will be processed shortly. Please try again.
        sleep(10)
        response = requests.post(url, data=encoded_request, headers=headers)
        bs = BeautifulSoup(response.content, features='lxml')

    table_summary = bs.find('summary')
    target = table_summary.find('target')
    table_body = bs.find('table')
    rows = table_body.find_all('tr')

    for row in rows[2:]:
        cols = row.find_all('td')
        index = row.find('th')
        extra = []
        extra += [index['web_id']] if index.has_attr('web_id') else [None]
        cols = [index.text.strip()] + extra + [str(x.text.strip()) for x in cols]
        raw_data += [cols]

    # Get the list of metrics names
    media_column = rows[0].find_all('td')
    header = rows[1].find_all('td')
    header = [str(rows[1].find('th').text.strip().lower()), 'web_id'] + header

    # Loop through the metrics data to put one metric and one media as one row
    for row in raw_data:
        if len(row) == len(header):
            for i in range(3, len(raw_data[0])):
                processed_data += [row[:3] + [header[i].text.strip(), header[i]['id'], row[i], media_column[i-2].text.strip(), target.text.strip()]]
        else:
            logger.info(row)

    df = get_processed_df(processed_data)

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse arguments passed')
    parser.add_argument("--role-arn", help="Role ARN to be used when reading & writing to S3.", required=True)
    parser.add_argument("--month", help="the execution day")
    parser.add_argument("--output-path", help="Output path")
    parser.add_argument("--table", help="table name")
    parser.add_argument("--api-call", help="api user name and password")
    parser.add_argument("--start-date", help="the date of the first available report")
    parser.add_argument("--start-month-id", help="the ID for the first report")
    parser.add_argument("--secrets-role", help="Secrets Role")

    args = parser.parse_args()
    output_path = args.output_path
    month = args.month[:-2]+'01'  # change the date to the first day 2019-01-01
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
    spark._jsc.hadoopConfiguration().set("amz-assume-role-arn", args.role_arn)
    spark.sql("set spark.hadoop.fs.s3.maxRetries=20")
    spark.sql("set parquet.compression=snappy")

    # Load config yaml
    time_frame_list = config_yaml['timeframe']
    targets = config_yaml['targets']
    measures = config_yaml['media_measures']
    reports = config_yaml['reports']

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

    data = []

    for time_frame in time_frame_list:
        for target_type in targets:
            df = get_data(month_id, time_frame['id'], base64string, target_type, measures, reports)
            df['month'] = datetime.strptime(month, '%Y-%m-%d').strftime('%B')
            df['year'] = month[:4]
            df['year'] = pd.to_numeric(df['year'])
            df['platform'] = 'Desktop'
            df['timeframe'] = time_frame['time']
            data.append(df)
    result = pd.concat(data, ignore_index=True)

    schema = StructType([StructField('benchmark_media', StringType(), nullable=True),
                         StructField('benchmark_media_id', StringType(), nullable=True),
                         StructField('demographic', StringType(), nullable=True),
                         StructField('comparison_media', StringType(), nullable=True),
                         StructField('media_target_audience', StringType(), nullable=True),
                         StructField('per_benchmark_media_visit_comparison_media', FloatType(), nullable=True),
                         StructField('per_comparision_media_visit_benchmark_media', FloatType(), nullable=True),
                         StructField('index', FloatType(), nullable=True),
                         StructField('shared_audience_000', FloatType(), nullable=True),
                         StructField('month', StringType(), nullable=True),
                         StructField('year', IntegerType(), nullable=True),
                         StructField('platform', StringType(), nullable=True),
                         StructField('timeframe', StringType(), nullable=True)])

    df = spark.createDataFrame(result, schema=schema)

    # Changed NaN to Null
    columns = df.columns
    for column in columns:
        df = df.withColumn(column, f.when(f.isnan(f.col(column)), None).otherwise(f.col(column)))

    destination_path = 's3://' + output_path + '/zillow/comscore/{table}/{month}/'.format(table=table, month=month)
    df.write.mode('overwrite').format('parquet').save(destination_path)

    sql_string = """
        ALTER TABLE comscore.{table_name} ADD IF NOT EXISTS PARTITION (data_date='{partition}') location '{path}'
        """.format(table_name=table, partition=month, path=destination_path)

    spark.sql(sql_string)
    spark.stop()
