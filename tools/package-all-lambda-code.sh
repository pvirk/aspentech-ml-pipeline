./tools/package-lambda-code.sh ingest-lambda
./tools/package-lambda-code.sh model-lambda
./tools/package-lambda-code.sh enhance-lambda
./tools/package-lambda-code.sh transform-lambda
./tools/package-lambda-code.sh sagemaker-dataprep-lambda
./tools/package-lambda-code.sh sagemaker-training-kickoff-lambda
./tools/package-lambda-code.sh sagemaker-endpoint-update-lambda
./tools/package-lambda-code.sh deliver-api-lambda
./tools/package-lambda-code.sh predict-api-lambda

rm -rf functions/source/ingest-lambda/node_modules
rm -rf functions/source/model-lambda/node_modules
rm -rf functions/source/enhance-lambda/node_modules
rm -rf functions/source/transform-lambda/node_modules
rm -rf functions/source/sagemaker-dataprep-lambda/node_modules
rm -rf functions/source/sagemaker-training-kickoff-lambda/node_modules
rm -rf functions/source/sagemaker-endpoint-update-lambda/node_modules
rm -rf functions/source/deliver-api-lambda/node_modules
rm -rf functions/source/predict-api-lambda/node_modules
rm -rf functions/source/aws-helpers/node_modules

#cp templates/* packages/