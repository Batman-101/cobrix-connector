# cobrix-connector
**Pull Request** - As pull request names are commit messages to the master branch,
so they can also follow the same pattern of the commit message.
Although GitHub auto generates the name of the PR from the branch name so
if you are following the branch naming conventions your PR will always be having your ticket number. So, we recommend leave that as default only.

Eg: _Merge feature/FINTWO-9999/sample-branch-name to main


[Cill Cobrix Overview]
    - Converts BIN to ORC format
##### Program arguments to provide

    -DconfigFile=cobrix.conf 
    -Denvironment=hdfs/gcs/${region}/${environment}.conf
    -DerrorMetaDataPath=${instance_id}/${region}/audit/in/cillerrorlog/
    -DbasePath=/${instance_id}
    -DcopybookPath=${instance_id}/${region}/${copybookPath}
    -DpartnKeyYrHr=${current_date}   
    -DwfName=${wfName}
    -DprocessName=${pipelineName}
    -Dregion=${region}
    -DtransactionPaths=${transactionPaths}
    -DstartOffset=${auditScanStartOffset}
    -Drange=${auditScanRange}  
    -DsparkOptions.spark.driver.maxResultSize=2G
