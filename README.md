# Overview

This project looks to document a service that demonstrates one approach to fetching, processing, and loading the data files created by the Gentrack data replication service into a replica RDBMS.

The tool has been written in a modular fashion with the intention that it could be extended, maintained and productionised by a 3rd party.

# Code Structure

## Key Objects
![Key Objects](/doco/images/codeobjects.png)

### Providers
The providers are an access layer to lower-level services

#### IRemoteEndpointService
This interface allows for an implementation of file fetching operations from a remote source

Current methods allow for

* List files at the source
* Download an object
* Delete an object

Currently, there is one implementation of this called S3Service to perform these actions with an AWS S3 bucket

#### IDatabaseService
This interface allows for an implementation of operations on the required RDBMS target

methods allow for

* Bulk Load a file to target database/table
* Bulk load a file to a temp table and then UPSERT

Currently, there is one implementation of this called SqlServerDatabaseService targeted at Microsoft SQL Server 2016+

#### ILocalCacheService
This interface implements operations to store and cache files in a local file system. 

Storing the file locally prior to using the target RDBMS's bulk insert functionality is seen as the most memory-efficient approach

Current methods allow for

* list files that were downloaded but not processed
* mark a file as "done" (renames file to add .done extension)
* get Local file object

#### FileObject
A simple class that contains common details of files stored in both the remote source and local cache

It should be expected that the source endpoint from the replication service stores files using the following path/naming scheme

```
/<database name>/<table name>/<file name>
```

The file object allows storing the file location as well as the path information is broken down to help downstream processes both local and remote

### Producers
A producer is targeted at performing the following logic

polling a source endpoint
fetching any files found
storing into the local cache
deleting file at the source
add file pointer to the shared queue

#### IFileProducer
This interface currently implements a single generic method expected to perform the actions above called "StartPolling"

Currently, there is one implementation of this called S3FileProducer which uses the providers implemented in the section above to achieve this logic


### Consumers
A consumer is targeted at performing the following logic

polling the shared queue
dequeuing a file entry with respect to concurrency
calling the appropriate load operation for the file entry
marking the file as "done"

#### IFullLoadFileConsumer
This interface currently implements a single generic method expected to perform the general actions called "StartPolling"

Currently, the single implementation of this called FullLoadFileConsumer will perform the actions above with the following additional logic

* looks for files starting with the name "LOAD" to ensure we only look at full load files
* if a file that does not begin with "LOAD" is found it is assumed to be a delta file, the consumer enters shutdown mode, the assumption is that the files are being added to the queue in the correct order and a delta file in the queue means all full load files have been processed
* the full load process works using multiple threads, given there is no issue with concurrency and ordering for the initial load this allows the initial baseline to be loaded as fast as possible

#### IDeltaFileConsumer
This interface currently implements a single generic method expected to perform the general actions called "StartPolling"

Currently, the single implementation of this called DeltaFileConsumer will perform the actions with the following additional logic

* looks for files that do not start with the name "LOAD" to ensure we only look at delta files
* if a file that begins with "LOAD" is found it is assumed to be a full load file, he consumer enters shutdown mode, the assumption is that the files are being added to the queue in the correct order and a full load file in the queue means that the replication process has been restarted and requires investigation
* Unlike the full load process, the delta process will only process and load a single file at a time to ensure concurrency

### Misc/Other objects
#### Program
This is the entry point for the solution

currently enforces that a command-line option of -t / -type is provided with one of the following values and will load and run the services based on this

"full load"
"delta"
the main method will also look for user input of "Q" and return at any time to shutdown

#### Startup
The startup class implements a standard configuration object using the appsettings.json file as well as dependency injection for all the services

## Basic workflow
![Code flow](/doco/images/codeflow.png)

# The appsettings.config file

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.EntityFrameworkCore": "Error"
    }
  },
  "ConnectionStrings": {
    "SqlServer": "Data Source=<hostname>;Initial Catalog=<initial database name>;User ID=<user>;Password=<password>"
  },
  "replicationBucket": "<S3 bucket name>",
  "replicationBucketPrefix": "<filter name if>",
  "ProjectName": "<project name>",
  "Environment": "<environment name>",
  "LocalCachePath": "<local folder to store cached files>",
  "ParallelFullLoadStreams": <number of streams for full load>,
  "AWS": {
    "Profile": "<aws profile name>",
    "Region": "<aws region>"
  }
}
```

### Logging
The recomended option is "Information" logging only
the "Debug" options is also supported

### ConnectionStrings
As per the example above, ensure that hostname, initial catalog, user id, password are populated
Currently only supports the SqlServer value

### replicationBucket
The name of the S3 Bucket where the files can be found

### replicationBucketPrefix
This parameter allows the filtering of objects from the S3 Bucket, it is recommended to only be used when testing and wanting to limit the process to a single file

e.g by adding a value such as
```
\customerdbname\Account\LOAD000001.csv
```

The service will only look for the specified object

### ProjectName
Currently unused

### Environment
Currently unused

### LocalCachePath
The local folder where the process can download and cache the replication files in to

### ParallelFullLoadStreams
The number of streams that should be used when processing full load files
the recommendation is to choose a number based on the resources of the target database instance

### AWS
#### AWS Profile
This is the name of the AWS profile that should be created inside the AWS Credentials file matching the access details required for the source S3 Bucket that should be used


# How to deploy

## Pre Requisites
* Install the AWS CLI and ensure the AWS Credentials file is setup
* Add an entry to the credentials file using the access key details provided by Gentrack
* Ensure there is enough space provisioned and a folder created/identified for the local cache (referenced in appsettings.config
* Ensure there is an SQLServer instance provisioned where an initial database has been created based on the schema details provided by Gentrack

## Running the service
1. Compile and create a release of the solution for the target architecture required
1. Create an appsettings.config file as per the details in the section above
1. Run the tool in full load mode (use -m "full")
1. Run the tool in delta mode (use -m "delta")


## License
[MIT](https://choosealicense.com/licenses/mit/)