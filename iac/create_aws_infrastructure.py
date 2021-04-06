import boto3
import configparser

from iam import *
from redshift import *

def main():
    """
    Main function for infrastructure as code creation. It reads the config file, creates the aws infrastructure (iam role, redshift db) and saves needed params, 
    like the iam role arn or redshift host url, to a config file.
    """
    # Load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('./dwh.cfg'))

    CLUSTER_TYPE            = config.get("CLUSTER","CLUSTER_TYPE")
    CLUSTER_NUM_NODES       = config.get("CLUSTER","CLUSTER_NUM_NODES")
    CLUSTER_NODE_TYPE       = config.get("CLUSTER","CLUSTER_NODE_TYPE")

    IDENTIFIER              = config.get("CLUSTER","IDENTIFIER")
    DB_NAME                 = config.get("CLUSTER","DB_NAME")
    DB_USER                 = config.get("CLUSTER","DB_USER")
    DB_PASSWORD             = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT                 = config.get("CLUSTER","DB_PORT")

    CLUSTER_IAM_ROLE_NAME   = config.get("CLUSTER", "CLUSTER_IAM_ROLE_NAME")
    
    # Load AWS credentials from a file
    config.read_file(open('./aws.cfg'))
    KEY                     = config.get('AWS','KEY')
    SECRET                  = config.get('AWS','SECRET')

    # Create clients for IAM, EC2, S3 and Redshift
    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iamRoleArn = create_iam_role(iam, CLUSTER_IAM_ROLE_NAME)
    print(iamRoleArn)
    
    dbEndpoint = create_redshift_cluster(redshift, CLUSTER_TYPE, CLUSTER_NODE_TYPE, CLUSTER_NUM_NODES, DB_NAME, IDENTIFIER, DB_USER, DB_PASSWORD, iamRoleArn)
    print(dbEndpoint)    
    
    # Load config from a file
    config = configparser.ConfigParser()
    config.read_file(open('./config.cfg'))
      
    CLUSTER = config['CLUSTER']
    CLUSTER['HOST'] = dbEndpoint
    CLUSTER['DB_NAME'] = DB_NAME
    CLUSTER['DB_USER'] = DB_USER
    CLUSTER['DB_PASSWORD'] = DB_PASSWORD
    CLUSTER['DB_PORT'] = DB_PORT
    
    IAM_ROLE = config['IAM_ROLE']
    IAM_ROLE['ARN'] = "'{}'".format(iamRoleArn)
    IAM_ROLE['test'] = "'test123'"
    
    #Write changes back to file
    with open('config.cfg', 'w') as conf:
        config.write(conf)
    
if __name__ == "__main__":
    main()
