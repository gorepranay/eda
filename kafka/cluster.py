import boto3
import time
import os
import configparser

curr_dir = os.path.dirname(os.path.realpath(__file__))
proj_dir = os.path.dirname(os.getcwd())
properties_file=os.path.join(proj_dir,'eda.properties')

kafka_client = boto3.client('kafka')

#Get properties file information
def get_config(properties_file, section):
    config = configparser.RawConfigParser()
    config.read(properties_file)
    return dict(config.items(section))

#Create MSK cluster
def create_msk():
    '''
    :return: dict('ClusterArn', 'ClusterName', 'State': 'ACTIVE' | 'CREATING' | 'UPDATING' | 'DELETING' | 'FAILED')
    '''
    response = kafka_client.create_cluster(MSKConfig.config)
    os.environ['MSK_CLUSTER_ARN']=response['ClusterArn']
    return response

# Get cluster metadata information
def describe_msk(cluster_arn):
    response_status = kafka_client.describe_cluster(ClusterArn=cluster_arn)
    while response_status['ClusterInfo']['State'] !='ACTIVE':
        time.sleep(2*60)
        response_status = kafka_client.describe_cluster(ClusterArn=cluster_arn)

    response_brokers = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    os.environ['MSK_ZOOKEEPER'] = response_status['ClusterInfo']['ZookeeperConnectString']
    os.environ['MSK_BROKER'] = response_brokers['BootstrapBrokerString']
    return {'zookeeperConnect':os.environ['MSK_ZOOKEEPER'],'brokers':os.environ['MSK_BROKER']}

def get_brokers():
    '''Get Brokers information from CLuster ARN in properties file'''
    arn=get_config(properties_file=properties_file, section='MSK')['cluster_arn']
    response_brokers = kafka_client.get_bootstrap_brokers(ClusterArn=arn)
    return response_brokers['BootstrapBrokerString']

class MSKConfig:
    config='''BrokerNodeGroupInfo={
        'BrokerAZDistribution': 'DEFAULT',
        'ClientSubnets': [
                "subnet-072cd5586e4bc3ff0",
                "subnet-0c3c859772d6b1b82"
            ],
        'InstanceType': 'kafka.t3.small',
        'SecurityGroups': ["sg-0e66b72848ab483de","sg-0d6eb1a1f3b880261","sg-01cc935026c7cbe0e"],
        'StorageInfo': {
            'EbsStorageInfo': {
                'VolumeSize': 2
            }
        }
    },
    ClientAuthentication={
        'Sasl': {
            'Scram': {
                'Enabled': False
            }
        },
        'Tls': {
            'CertificateAuthorityArnList': [
                'string',
            ]
        }
    },
    ClusterName='eda-kafka',
    CurrentBrokerSoftwareInfo={KafkaVersion': '2.2.1
    ConfigurationInfo={
        'Arn': 'string',
        'Revision': 123
    },
    'EncryptionInfo'={
            'EncryptionAtRest'= {
                'DataVolumeKMSKeyId'='arn:aws:kms:eu-west-1:632943041262:key/8e9d09dc-016e-41b5-b90f-405c66264e79'
            },
            'EncryptionInTransit'={
                'ClientBroker':'TLS_PLAINTEXT',
                'InCluster': True
            }
        },
    EnhancedMonitoring='DEFAULT',
    OpenMonitoring={
        'Prometheus': {
            'JmxExporter': {
                'EnabledInBroker': False
            },
            'NodeExporter': {
                'EnabledInBroker': False
            }
        }
    },
    KafkaVersion='string',
    LoggingInfo={
        'BrokerLogs': {
            'CloudWatchLogs': {
                'Enabled': True,
                'LogGroup': 'EDA-MVP'
            },
            'Firehose': {
                'DeliveryStream': 'string',
                'Enabled': False
            },
            'S3': {
                'Bucket': 'string',
                'Enabled': False,
                'Prefix': 'string'
            }
        }
    },
    NumberOfBrokerNodes=2,
    Tags={
        'string': 'string'
    }]
    '''