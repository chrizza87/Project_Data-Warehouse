import json

def create_iam_role(iam, iamRoleName):
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=iamRoleName,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    iam.attach_role_policy(RoleName=iamRoleName,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
    
    print('IAM role created')

    return iam.get_role(RoleName=iamRoleName)['Role']['Arn']

def delete_iam_role(iam, iamRoleName):
    try:
        iam.detach_role_policy(RoleName=iamRoleName, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=iamRoleName)
        print('IAM role deleted')

    except Exception as e:
        print(e)
        
def get_iam_role_arn(iam, iamRoleName):
    return iam.get_role(RoleName=iamRoleName)['Role']['Arn']
    