# quickstart-datalake-pariveda

This GitHub repository contains a quickstart template for creating a datalake in AWS integrated with Sagemaker to derive insights using machine learning.

## Development Command Line Tools

Note: All command line tools should be run in root directory of project

- Create/Update Stack: `tools/create-update-stacks.sh [stack-suffix]`

- Upload Lambda Code to S3: `tools/upload-lambda-code.sh [lambda-name]`

- Update Lambda Code: `tools/update-lambda-code.sh [function-name] [lambda-name]`

- Send Data to Kinesis: `tools/send-kinesis-data.sh [stream-name]`


## CI Using taskcat ##

To test multi-region deployment using the [AWS TaskCat tool](https://github.com/aws-quickstart/taskcat), first prepare the code for publishing

`./tools/publish-all-lambda-code.sh`

Then run taskcat from the parent directory.  

`cd ..`

`taskcat -c quickstart-datalake-pariveda/ci/config.yml`

This will test the script in all regions that support SageMaker: 
- us-east-1
- us-east-2
- us-west-2
- ap-northeast-1
- ap-northeast-2
- ap-southeast-2
- eu-central-1
- eu-west-1