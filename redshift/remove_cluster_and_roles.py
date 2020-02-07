
import boto3
import configparser

def main():

   # Load DWH Params from config file
   config_aws_cred = configparser.ConfigParser()
   try:
      config_aws_cred.read_file(open('aws_do_not_share.cfg'))
   except Exception as e:
      print(e)
      
   # load redshift cluster config settings from config file
   config_redshift = configparser.ConfigParser()
   try:
     config_redshift.read_file(open('aws_setup.cfg'))
   except Exception as e:
     print(e)

   # Load DWH Params from config file
   config_dwh = configparser.ConfigParser()
   try:
      config_dwh.read_file(open('dwh.cfg'))
   except Exception as e:
      print(e)

   KEY                    = config_aws_cred.get('AWS','KEY')
   SECRET                 = config_aws_cred.get('AWS','SECRET')

   DWH_CLUSTER_IDENTIFIER = config_redshift.get("SETUP","DWH_CLUSTER_IDENTIFIER")

   DWH_IAM_ROLE_NAME      = config_redshift.get("SETUP", "DWH_IAM_ROLE_NAME")

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
   try:
      print('Delete Cluster')
      redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
   except Exception as e:
      print(e)
   
   try:
      print('Detach role policy')
      iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
   except Exception as e:
      print(e)

   try:
      print('Delete IAM role')
      iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
   except Exception as e:
      print(e)

if __name__ == "__main__":
    main()

