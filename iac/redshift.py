def create_redshift_cluster(redshift, clusterType, nodeType, numNodes, dbName, clusterIdentifier, dbUser, dbPassword, iamRoleArn):
    # Create a RedShift Cluster
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=clusterType,
            NodeType=nodeType,
            #NumberOfNodes=int(numNodes),

            #Identifiers & Credentials
            DBName=dbName,
            ClusterIdentifier=clusterIdentifier,
            MasterUsername=dbUser,
            MasterUserPassword=dbPassword,
            
            #Roles (for s3 access)
            IamRoles=[iamRoleArn]  
        )
        
        waiter = redshift.get_waiter('cluster_available')
        waiter.wait(ClusterIdentifier=clusterIdentifier)
        
        print('Redshift cluster created')
        
        return response['Endpoint']['Address']
    except Exception as e:
        print(e)

def delete_redshift_cluster(redshift, clusterIdentifer):  
    try:
        redshift.delete_cluster( ClusterIdentifier=clusterIdentifer,  SkipFinalClusterSnapshot=True)
        waiter = redshift.get_waiter('cluster_deleted')
        waiter.wait(ClusterIdentifier=clusterIdentifer)
        
        print('Redshift cluster deleted')
    except Exception as e:
        print(e)
