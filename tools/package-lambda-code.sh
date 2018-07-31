#!/bin/sh

lambda=$1

source="functions/source/$lambda"
filename="$lambda.zip"
package_folder="functions/packages"
destination="$package_folder/$lambda/$filename"

cd $source

s3Key="$lambda/$filename"

npm install
zip -r $lambda .

cd "../../.."

mkdir -p $package_folder/$lambda/
mv "$source/$filename" $destination
