import boto3
import json
import configparser
import time

# Create IAM Role
def create_iam_role(iam,DWH_IAM_ROLE_NAME):
  try:
      print("Creating a new IAM Role") 
      dwhRole = iam.create_role(
          Path='/',
          RoleName=DWH_IAM_ROLE_NAME,
          Description = "Allows Redshift clusters to call AWS services on your behalf.",
          AssumeRolePolicyDocument=json.dumps(
              {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
              'Version': '2012-10-17'})
      )    
  except Exception as e:
      print(e)

# Attach Policy to IAM Role
def attach_policy_to_role(iam,DWH_IAM_ROLE_NAME):
  try: 
    print('Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
  except Exception as e:
      print(e)

# Create new Redshift cluster
def create_cluster(redshift,iam,DWH_CLUSTER_TYPE,DWH_NODE_TYPE,DWH_NUM_NODES,DWH_DB,DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD,DWH_IAM_ROLE_NAME):
  try:
    print('Creating Cluster')
    response = redshift.create_cluster(        
        # add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # add parameters for identifiers & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        # add parameter for role (to allow s3 access)
        IamRoles=[iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']]  
    )
  except Exception as e:
    print(e)

  try:
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print("Cluster created")
  except Exception as e:
    print(e)


def wait_until_cluster_available(redshift, DWH_CLUSTER_IDENTIFIER):
  n=0
  while True: 
      myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
      if myClusterProps.get('ClusterStatus') == 'available':
          print("Done.")
          break
      else:
          print("Waiting for cluster to be available...", n)
          time.sleep(30)
          n=n+1

# store endpoint and role_arn in config file
def update_config_dwh(config_dwh, redshift,DWH_CLUSTER_IDENTIFIER):
  
  # retrieve endpoint and role_arn
  myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
  DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
  DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
  
  # add to config object
  config_dwh.set('CLUSTER', 'HOST', DWH_ENDPOINT)
  config_dwh.set('IAM_ROLE', 'ARN', "'" + DWH_ROLE_ARN + "'")
  
  # write variables to config object
  try:
    print('Update config file')
    with open('dwh.cfg', 'w') as configfile:
      config_dwh.write(configfile)
  except Exception as e:
    print(e)

def open_incoming_tcp_port(redshift,ec2,DWH_CLUSTER_IDENTIFIER,DWH_PORT):
  try:
    print('Opening incoming tcp port')
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName='default',
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
  except Exception as e:
    print(e)

def main():
  # Load credentials from config file
  config_aws_cred = configparser.ConfigParser()
  try:
    config_aws_cred.read_file(open('redshift/aws_do_not_share.cfg'))
  except Exception as e:
    print(e)
  
  # load redshift cluster config settings from config file
  config_redshift = configparser.ConfigParser()
  try:
    config_redshift.read_file(open('redshift/aws_setup.cfg'))
  except Exception as e:
    print(e)

  # Load DWH Params from config file
  config_dwh = configparser.ConfigParser()
  try:
    config_dwh.read_file(open('redshift/dwh.cfg'))
  except Exception as e:
    print(e)

  KEY                    = config_aws_cred.get('AWS','KEY')
  SECRET                 = config_aws_cred.get('AWS','SECRET')

  DWH_CLUSTER_TYPE       = config_redshift.get("SETUP", "DWH_CLUSTER_TYPE")
  DWH_NUM_NODES          = config_redshift.get("SETUP","DWH_NUM_NODES")
  DWH_NODE_TYPE          = config_redshift.get("SETUP","DWH_NODE_TYPE")
  DWH_CLUSTER_IDENTIFIER = config_redshift.get("SETUP","DWH_CLUSTER_IDENTIFIER")
  DWH_IAM_ROLE_NAME      = config_redshift.get("SETUP", "DWH_IAM_ROLE_NAME")
  
  DWH_DB                 = config_dwh.get("CLUSTER","DB_NAME")
  DWH_DB_USER            = config_dwh.get("CLUSTER","DB_USER")
  DWH_DB_PASSWORD        = config_dwh.get("CLUSTER","DB_PASSWORD")
  DWH_PORT               = config_dwh.get("CLUSTER","DB_PORT")

  # Create clients for EC2, S3, IAM, and Redshift
  ec2 = boto3.resource('ec2',
                          region_name="us-west-2",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET
                      )

  iam = boto3.client('iam',
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET,
                      region_name='us-west-2'
                    )

  redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
  
  create_iam_role(iam,DWH_IAM_ROLE_NAME)
  attach_policy_to_role(iam, DWH_IAM_ROLE_NAME)
  create_cluster(redshift,iam,DWH_CLUSTER_TYPE,DWH_NODE_TYPE,DWH_NUM_NODES,DWH_DB,DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD,DWH_IAM_ROLE_NAME)
  wait_until_cluster_available(redshift, DWH_CLUSTER_IDENTIFIER)
  update_config_dwh(config_dwh, redshift,DWH_CLUSTER_IDENTIFIER)
  open_incoming_tcp_port(redshift,ec2,DWH_CLUSTER_IDENTIFIER,DWH_PORT)

if __name__ == "__main__":
    main()




