# Data capture system in AWS

This is a set of Amazon cloud (AWS) Lambda functions (in python) that are used by the *Gabinete Compartilhado of
Movimento Acredito* to periodically download information from the internet and store it in Amazon
and Google clouds.

The system is intended to be generic and flexible enough to allow capturing
(through HTTP requests) any data on the internet. For most cases, you should only need to edit
the `capture_urls.json` file, expected to be placed in AWS dynamoDB. However, some code editing might be required.

**NOTE:** This project is not easily installed and used since it requires a manual and farily long
setup process in AWS. We only provide the Lambda function codes and a dynamoDB table used as
configuration. Moreover, some capturing processes also rely on AWS Athena queries, Google storage and BigQuery
that are currently not available here.

### Basic notes on setting the system up on AWS

On top of placing the python codes in Lambda functions, changing their address in invocations
and adjusting the memory size according to the functions' needs, one key aspect of the system
is setting up time triggers with CloudWatch events. These are shown in `capture_system_flowchart.png`.

### Description of files and folders

* `capture_system_flowchart.png`: a flowchart roughly describing how the multiple Lambda functions interact to
capture information from the internet, along with other services such as triggers and databases.

* `backup-lambda.sh`: a shell script that uses the `aws` shell command to download the latest (`$LATEST`) version of Lambda
functions implemented in the system.

* `lambda`: the folder containing the Lambda function's codes and some info about them.

* `dynamoDB`: the tables (json files) needed by the capturing process, originally stored in AWS dynamoDB.

### Authors

* João Carabetta - [@JoaoCarabetta](https://github.com/JoaoCarabetta)

* Henrique S. Xavier - [@hsxavier](https://github.com/hsxavier)
