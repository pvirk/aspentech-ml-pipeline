./tools/package-lambda-code.sh ingest-lambda
./tools/package-lambda-code.sh model-lambda
./tools/package-lambda-code.sh enhance-lambda
./tools/package-lambda-code.sh transform-lambda
./tools/package-lambda-code.sh sagemaker-dataprep-lambda
./tools/package-lambda-code.sh sagemaker-training-kickoff-lambda
./tools/package-lambda-code.sh sagemaker-endpoint-update-lambda
./tools/package-lambda-code.sh deliver-api-lambda
./tools/package-lambda-code.sh predict-api-lambda

rm -rf src/ingest-lambda/node_modules
rm -rf  src/model-lambda/node_modules
rm -rf  src/enhance-lambda/node_modules
rm -rf  src/transform-lambda/node_modules
rm -rf  src/sagemaker-dataprep-lambda/node_modules
rm -rf  src/sagemaker-training-kickoff-lambda/node_modules
rm -rf  src/sagemaker-endpoint-update-lambda/node_modules
rm -rf  src/deliver-api-lambda/node_modules
rm -rf  src/predict-api-lambda/node_modules
rm -rf  src/aws-helpers/node_modules

cp templates/* packages/