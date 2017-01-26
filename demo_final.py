# Common Azure modules
import adal
import azure.mgmt.resource.resources
import msrestazure.azure_active_directory

# Azure Data Lake modules
import azure.mgmt.datalake.store
import azure.datalake.store
import azure.mgmt.datalake.analytics

# Other useful modules
import os
import sys
import itertools

# the adlhelper will simplify working with Azure Data Constants 
import adlhelper

print("DEMO STARTING")

# define constants
tenant = "microsoft.onmicrosoft.com"
subscription_id = '045c28ea-c686-462f-9081-33c34e871ba3'
adls_account_name = 'datainsightsadhoc'
adla_account_name = 'datainsightsadhoc'

# Handle Authentication
token_cache_fname = r"C:\src\tr23python\adl_demo_tokencache.pickle"
auth_session = adlhelper.AuthenticatedSession( subscription_id , tenant, token_cache_fname )
token = auth_session.Token
credentials =  auth_session.Credentials

# Client construction
resource_clients = adlhelper.DataLakeResourceClients( auth_session )
store_clients = adlhelper.DataLakeStoreClients( auth_session, adls_account_name)
analytics_clients = adlhelper.DataLakeAnalyticsClients( auth_session )

# Use the Clients ------------------------------------------------

# List ADLA Accounts
adla_accounts = resource_clients.AnalyticsAccountClient.account.list()
for a in adla_accounts:
    print("ADLA: " + a.name)

# List ADLS Accounts
adls_accounts = resource_clients.StoreAccountClient.account.list()
for a in adls_accounts:
    print("ADLS: " + a.name)

# List Files in ADLS
files = store_clients.FileSystemClient.ls( "/system" )
for f in files:
    print(f)

# List 10 Jobs in ADLS

jobs = analytics_clients.JobClient.job.list( adla_account_name )
jobs = itertools.islice(jobs,10) # comment this out if you want all the jobs
for j in jobs:
    print("---------------------------------")
    print(j.name)
    print(j.submit_time)
    print(j.submitter)

# Submit a job

usqlscript = 'somescript'

jobId = str(uuid.uuid4())
jobResult = analytics_clients.JobClient.job.create(
    adla_account_name,
    jobId,
    azure.mgmt.datalake.analytics.job.models.JobInformation(
        name='MyJob',
        type='USql',
        properties=azure.mgmt.datalake.analytics.job.models.USqlJobProperties(
            script=usqlscript 
        )
    )
)

print("DEMO FINISHED")