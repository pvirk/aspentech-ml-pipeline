#!/bin/sh

# pip install virtualenv -q
# virtualenv venv
# source venv/bin/activate
pip install -r tools/requirements.txt -q


stack_suffix="master"
if [ $# -eq 0 ]
	then
    	echo "No stack suffix argument provided. Using master."
   	else
   		stack_suffix="$1"
fi

export bucket_name = "s3://datalake-sagemaker-demo-lambda-code"
export transform_bucket = "s3://imet-sagemaker-demo-data-lake-$stack_suffix"


aws s3 cp "templates/lambdacode.template" "$bucket_name/lambdacode.template"
aws s3 cp "templates/deliver.template" "$bucket_name/deliver.template"
aws s3 cp "templates/imet.template" "$bucket_name/imet.template"
aws s3 cp "templates/sagemaker.template" "$bucket_name/sagemaker.template"

python tools/create-update-stack.py "datalake-sagemaker-demo" "templates/master.template" "ci/datalake-sagemaker.json" $stack_suffix

aws s3 sync "$bucket_name/data/transform" "$transform_bucket/transform"

deactivate