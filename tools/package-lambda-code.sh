#!/bin/sh

lambda=$1
bucketName="s3://datalake-sagemaker-demo-lambda-code"

source="src/$lambda"
filename="$lambda.zip"
package_folder="packages"
destination="$package_folder/$lambda/$filename"

cd $source

s3Key="$lambda/$filename"

npm install
zip -r $lambda .

cd "../.."

mkdir -p $package_folder/$lambda/
mv "$source/$filename" $destination
