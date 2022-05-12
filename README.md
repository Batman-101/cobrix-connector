# cill-cobrix-connector

[![Build Status](https://ci.wcnp.walmart.com/buildStatus/icon?job=finance-technology-cill%2Fcill-cobrix-connector%2Fdev)](https://https://ci.wcnp.walmart.com/job/finance-technology-cill/job/cill-cobrix-connector/job/dev/)

[![Quality Gate Status](https://sonar.wal-mart.com/api/project_badges/measure?project=finance-technology-cill%3Acill-cobrix-connector&metric=alert_status)](https://sonar.wal-mart.com/dashboard?id=finance-technology-cill%3Acill-cobrix-conector)

## [**Hygieia**]()

### Releases - [What's new ??](RELEASES.md)

### Jira Integration
Use the following naming convention to sync your commits, branches and PRs to your JIRA User Story.

**Commit Naming Convention** - `<USER_STORY> comment`

Eg: _FINTWO-9999 Add alert module support_


**Branch Naming Convention** - `<BRANCH_TYPE>/<USER_STORY>/<description>`

Regex: `^((feature|fix|doc|test)\/FINTWO\-[0-9]{4,5}\/[a-z][a-z0-9\-]*[a-z])$`

Eg: _feature/FINTWO-9999/sample-branch-name_


**Pull Request** - As pull request names are commit messages to the master branch,
so they can also follow the same pattern of the commit message.
Although GitHub auto generates the name of the PR from the branch name so
if you are following the branch naming conventions your PR will always be having your ticket number. So, we recommend leave that as default only.

Eg: _Merge feature/FINTWO-9999/sample-branch-name to main

## Summary:
Walmart's Financial Retail Enablement FinTech group responsible for providing the systems / process to complete a
store's/warehouse cash finalization. The Cash Finalization process is what each store/warehouse must complete to
finalize their cash flow each day from the perspective of on-hand cash movement, sales, returns, etc.

[Cill Cobrix Overview](https://https://collaboration.wal-mart.com/display/FINTECH/CILL+Cobrix+connector+-+overview+and+usage)
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