# Changelog

All notable changes to this project will be documented in this file.

## v1.0.0

### Commits

- **[WIT-249] Some SPs provisioning status endpoint incorrectly reports completed for non existent tokens**
  > 
  > Closes WIT-249

- **[WIT-196] Incorrect usage of inline constraints for composite primary keys**

- **[#17, #20] Syntax errors result in unintuitive errors & API versioning**

- **Resolve WIT-116 "Change es hasuraspecificprovisionersnowflake"**

- **[#18] Documentation**

- **Resolve WIT-51 "Update snowflake helm chart"**

- **feature: integrate-fake-secret-store**

- **[#15] Resolve "DataContract Schema constraints not compliant to Policy"**
  > 
  > ##### Bug fixes
  > 
  > - Fixed DataContract Schema constraints: updated the supported values in order to match the ones specified inside the policies
  > - Updated CI to skip the deploy-dev step (since it is currently unused)
  > 
  > ##### Related issue
  > 
  > Closes #15

- **[#14] Resolve "Fix UpdateAcl Snowflake queries"**
  > 
  > ##### New features and improvements
  > 
  > - Added role deletion on Output port unprovisioning
  > - Added a buildRoleName method to provide a better role name creation in order to avoid naming conflicts (when deploying multiple Data products)
  > 
  > ##### Bug fixes
  > 
  > - Fixed privileges assignment to role and in particular added the execution of all the SQL queries needed in order to have a role inside Snowflake working for the user as expected. See [this resource](https://community.snowflake.com/s/article/Solution-Grant-access-to-specific-views-in-SNOWFLAKE-ACCOUNT-USAGE-to-custom-roles) for details.
  > 
  > ##### Related issue
  > 
  > Closes #14

- **[#13] Resolve "Fix UpdateAcl"**
  > 
  > ##### Bug fixes
  > 
  > - Fixed mapUserToSnowflakeUser method in order to provide also username capitalization
  > - Fixed buildRefsStatement method in order to use the correct viewName when building the ASSIGN_ROLE statement
  > - Added inside the Output Port provisioning the invocation of UpdateAcl in order to grant access for the created view to the dataProductOwner
  > 
  > ##### Related issue
  > 
  > Closes #13

- **[#10] Resolve "Custom view validation"**
  > 
  > ##### New features and improvements
  > 
  > When a custom view query is provided:
  > * Added a new method to compare the schemas between the dataContract and the custom one. If they don't match, the view creation will fail.
  > * Added a check to match the viewName with the custom one. If they don't match, the view creation will fail.
  > * Updated the code to use the right information (dbName, schemaName, viewName) also for the unprovision and updateACL.
  > 
  > Other changes:
  > * Updated 1 SQL query related to the updateACL feature in order to map the received username string to the corresponding Snowflake user (according to this processing: removed the "user:" prefix, if present and replaced the last underscore with an "@" to restore the original user email that will be used as Snowflake username in our use case).
  > * Improved SP logs
  > 
  > ##### Related issue
  > 
  > Closes #10

- **[#11] Resolve "Update updateAcl API"**
  > 
  > ##### New features and improvements
  > 
  > * Updated grant privileges query in order to use the view instead of a table, to reflect the changes made for the output port provisioning.
  > 
  > ##### Related issue
  > 
  > Closes #11

- **[WIP] feature: custom-sandbox-changes**

- **[#9] Resolve "Add a new method for creating a view"**
  > 
  > ##### New features and improvements
  > 
  > For output port:
  > * Replaced table creation with view creation on Snowflake (modified descriptors and tests accordingly)
  > * Added the possibility for the user to manually specify the optional field "customView" with a string that will contain the query that be eventually used for the view creation (that will be custom in this case) instead of the default one (that uses the view name, the data product schema for fields and table name as source table to create the view on top of it)
  > 
  > ##### Related issue
  > 
  > Closes #9

- **[#8] Resolve "improve database schema name"**
  > 
  > ##### Bug fixes
  > 
  > * Update expression to get the default database schema name (if not provided by the user)
  > 
  > ##### Related issue
  > 
  > Closes #8

- **[#7] Resolve "Extend provision API"**
  > 
  > ##### New features and improvements
  > 
  > * Implemented /provision and /unprovision for Snowflake Storage component
  > * Updated Provisioning request descriptor examples and added some for the Storage component
  > 
  > ##### Related issue
  > 
  > Closes #7

- **[#6] Resolve "Implement unprovision API"**
  > 
  > ##### New feature and improvements
  > 
  > - Implemented the unprovision API
  > 
  > ##### Related issues
  > 
  > Closes #6

- **[#5] Resolve "Implement provision API"**
  > 
  > ##### New features and improvements
  > 
  > - Implemented provision API
  > - Modified updateAcl
  > 
  > ##### Related issues
  > 
  > Closes #5

- **[#4] Resolve "Implement updateAcl API"**
  > 
  > ##### New features and improvements
  > 
  > - Added updateAcl API
  > 
  > ##### Related issues
  > 
  > Closes #4

- **Resolve "Yaml parsing"**
  > 
  > ##### New features and improvements
  > 
  > - Addition of the Yaml parser for the input coming from the Provisioning Coordinator. In particular, based on the Provisioning request, implemented the following descriptor classes (whose names indicate the elements that they are modeling): ProvisioningRequestDescriptor, DataProductDescriptor, ComponentDescriptor.
  > - Addition of some tests, based on examples of valid/invalid Yaml Provisioning requests
  > 
  > ##### Related issue
  > 
  > Closes #2

- **[#3] Resolve "Import AWS SDK"**
  > 
  > ##### New feature and improvement
  > 
  > This MR introduces the AWS SDK for writing and reading inside an S3 bucket.
  > In order to use it, you need to import the code from the `aws-integration` module and update the dependencies of your specific provisioner's main module.
  > You need also to specify an `is-mock` parameter inside `reference.conf` that is telling you if you want a mocked version of the aws SDK or not. In case of not, you need to set up some Environment variables in order to authenticate to AWS:
  > 
  > - `AWS_REGION`: the region in which the S3 bucket is created
  > - `AWS_ACCESS_KEY_ID`: taken from your aws account
  > - `AWS_SECRET_ACCESS_KEY`: taken from your aws account
  > - `AWS_SESSION_TOKEN`: taken from your aws account (only in case of temporary authentication)
  > 
  > ##### Related Issue
  > 
  > Closes #3

- **Add MR standard template**

- **[#1] Snowflake JDBC**
  > 
  > Imported JDBC connector library in the project

- **Initial commit**
