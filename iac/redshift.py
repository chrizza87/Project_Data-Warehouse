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
        
        cluster = response['Cluster']
        return cluster['Endpoint']['Address']
    except Exception as e:
        print(e)
        return get_redshift_cluster(redshift, clusterIdentifier)

def delete_redshift_cluster(redshift, clusterIdentifier):  
    try:
        redshift.delete_cluster( ClusterIdentifier=clusterIdentifier,  SkipFinalClusterSnapshot=True)
        waiter = redshift.get_waiter('cluster_deleted')
        waiter.wait(ClusterIdentifier=clusterIdentifier)
        
        print('Redshift cluster deleted')
    except Exception as e:
        print(e)
        
def get_redshift_cluster(redshift, clusterIdentifier):  
    try:
        response = redshift.describe_clusters(ClusterIdentifier=clusterIdentifier)
        cluster = response['Clusters'][0]
        return cluster['Endpoint']['Address']
    except Exception as e:
        print(e)
