import json;
import boto3;

client = boto3.client('emr', region_name='<your-aws-region>',
aws_access_key_id='<your-aws-key>',aws_secret_access_key='<your-aws-access-key>')


def lambda_handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key'] # name of your raw files
    bucketName=event['Records'][0]['s3']['bucket']['name'] # name of your raw file bucket
    
    # location of code to run
    backend_code="s3://<code-bucket>/backend_code_aws.py"
    # define the job details, location of script to run etc
    spark_submit = [
    'spark-submit',
    '--master', 'yarn',
    '--deploy-mode', 'cluster',
    backend_code,
    bucketName,
    file_name
    ]
    print("Spark Submit : ",spark_submit)
    # define the cluster
    cluster_id = client.run_job_flow(
    Name="transient_demo_testing",
    Instances={
    'InstanceGroups': [
    {
    'Name': "Master",
    'Market': 'ON_DEMAND',
    'InstanceRole': 'MASTER',
    'InstanceType': 'm5.xlarge',
    'InstanceCount': 1,
    },
    {
    'Name': "Slave",
    'Market': 'ON_DEMAND',
    'InstanceRole': 'CORE',
    'InstanceType': 'm5.xlarge',
    'InstanceCount': 2,
    }
    ],
    'Ec2KeyName': 'airflow_key',
    'KeepJobFlowAliveWhenNoSteps': False,
    'TerminationProtected': False,
    'Ec2SubnetId': 'subnet-418b8927',
    },
    LogUri="s3://<your-log-bucket>/elasticmapreduce/",
    ReleaseLabel= 'emr-5.33.1',
    Steps=[{"Name": "testJobGURU",
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
    'Jar': 'command-runner.jar',
    # passed to the backend code
    'Args': spark_submit
    }
    }],
    BootstrapActions=[], # INSTALL ADDITIONAL SOFTWARE ON CLUSTER
    
    VisibleToAllUsers=True,
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    Applications = [ {'Name': 'Spark'},{'Name':'Hive'}])
    # All applications like Hive, Livy, Hadoop