import io


def gen(top):    
    output = io.StringIO()
   
    n = 1
        
    files = []

    print("// BEGIN ------------------------------------------------------------",file=output)
   
    for a in range(1,101):    
        for b in range(1,101):
            for c in range(1,101):
                if (n>top) : break             
                s = '"adl://adltrainingsampledata.azuredatalakestore.net/ManyFiles/%s/%s/%s/data_%s.csv"' % (a,b,c,n)
                files.append(s)
                n = n +1
    
    files = " , ".join( files )
    
    s = '@rows = EXTRACT a int, b int, c int, num int FROM %s USING Extractors.Csv();' % (files)               
    
    print(s,file=output)
    s = 'OUTPUT @rows TO "/manyfiles/output/manyfiles_%s.csv" USING Outputters.Csv();' % (top)
    print(s,file=output)
    
    print("// END ------------------------------------------------------------",file=output)

    return output.getvalue()


# Common Azure imports

import adal
import azure.mgmt.resource.resources
import msrestazure.azure_active_directory
import azure.mgmt.datalake.store
import azure.datalake.store
import azure.mgmt.datalake.analytics
import os
import sys
import itertools
import adlhelper
import uuid


# define constants
tenant = "microsoft.onmicrosoft.com"
subscription_id = 'ace74b35-b0de-428b-a1d9-55459d7a6e30'
adls_account_name = 'saveenrtr24store'
adla_account_name = 'saveenrtr24analytics'

# Handle Authentication
auth_session = adlhelper.AuthenticatedSession( subscription_id , tenant, r"d:\adl_demo_tokencache.pickle")
token = auth_session.Token
credentials =  auth_session.Credentials

# Client construction
resource_clients = adlhelper.DataLakeResourceClients( auth_session )
store_clients = adlhelper.DataLakeStoreClients( auth_session, adls_account_name)
analytics_clients = adlhelper.DataLakeAnalyticsClients( auth_session )


x
ns = [i for i in range(1,11)]
ns = ns + [i*10 for i in range(2,11)]
ns = ns + [i*100 for i in range(2,11)]
ns = ns + [i*1000 for i in range(2,11)]
 
print(ns)

for n in ns:
    usqlscript = gen(n) 
    print(usqlscript )

    jobId = str(uuid.uuid4())
    jobResult = analytics_clients.JobClient.job.create(
        adla_account_name,
        jobId,
        azure.mgmt.datalake.analytics.job.models.JobInformation(
            name='Many_Files_TEST4_%s' % n,
            type='USql',
            properties=azure.mgmt.datalake.analytics.job.models.USqlJobProperties(
                script=usqlscript 
            )
        )
    )
    