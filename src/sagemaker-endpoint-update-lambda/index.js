var AWS = require('aws-sdk');
var async = require('async');

exports.handler = (event, context, callback) => {
	var awsRegion = process.env.AWS_REGION;
	console.log('Received event:', JSON.stringify(event, null, 2));
    async.each(event.Records, function(record, callback){
		createUpdateEndpoint(awsRegion, record, callback);
	}, function(err){
		if(err) {
            console.log("Failed to process events.");
            context.fail("Failed to process events" + err);
        }
        else {
            console.log("Successfully processed " + event.Records.length + " records.");
            context.succeed("Successfully processed " + event.Records.length + " records.");
        }
	});
};

function createUpdateEndpoint(awsRegion, record, callback){
	var s3Bucket = record.s3.bucket.name;
	var key = record.s3.object.key;
	var s3Url = `s3://${s3Bucket}/${key}`;
	var keySplit = key.split("/");
	var dateString = keySplit[1];
	createModel(awsRegion, s3Url, dateString, function(err){
		if(err){
			callback(err);
		}
		else{
			createEndpointConfiguration(awsRegion, dateString, function(err){
				if(err){
					callback(err);
				}
				else{
					checkForEndpoint(awsRegion, function(err, endpointName){
						if(err){
							callback(err);
						}
						else if(endpointName){
							updateEndpoint(awsRegion, dateString, function(err){
								if(err){
									callback(err);
								}
								else{
									callback(null);
								}
							});
						}
						else{
							createEndpoint(awsRegion, dateString, function(err){
								if(err){
									callback(err);
								}
								else{
									callback(null);
								}
							})
						}
					});
				}
			});
		}
	});
}

function getModelCreationParams(s3Url, dateString){
	var modelName = getModelName(dateString);
	var modelExecutionRoleArn = process.env.ModelRoleArn;
	var modelImage = process.env.DeepARImage;
	var params = {
  		ExecutionRoleArn: modelExecutionRoleArn,
  		ModelName: modelName,
  		PrimaryContainer: {
    		Image: modelImage,
    		ModelDataUrl: s3Url
  		}
	};
	return params;
}

function getModelName(dateString){
	var modelNamePrefix = "ec2-spot-data-model-";
	return modelNamePrefix + dateString;
}

function getEndpointConfigurationName(dateString){
	var endpointConfigurationPrefix = "ec2-spot-data-endpoint-config-";
	return endpointConfigurationPrefix + dateString;
}

function getEndpointName(){
	return process.env.SageMakerEndpointName;
}

function createModel(awsRegion, s3Url, dateString, callback){
	var modelCreationParams = getModelCreationParams(s3Url, dateString);
	var sagemaker = new AWS.SageMaker({region: awsRegion});
	sagemaker.createModel(modelCreationParams, function(err, response){
		if(err){
			console.log(`Failed to create model: ${JSON.stringify(err)}`);
			callback(err);
		}
		else{
			console.log(`Successfully created model: ${JSON.stringify(response)}`);
			callback(null);
		}
	});
}

function getEndpointConfigurationParams(dateString){
	var endpointConfigName = getEndpointConfigurationName(dateString);
	var modelName = getModelName(dateString);
	var initialInstanceCount = parseInt(process.env.InitialEndpointInstanceCount);
	var InstanceType = process.env.EndpointInstanceType;
	var params = {
  		EndpointConfigName: endpointConfigName,
  		ProductionVariants: [
    		{
      			InitialInstanceCount: initialInstanceCount,
      			InstanceType: InstanceType,
      			ModelName: modelName,
      			VariantName: 'AllTraffic'
    		}
  		]
	};
	return params;
}

function createEndpointConfiguration(awsRegion, dateString, callback){
	var endpointConfigurationParams = getEndpointConfigurationParams(dateString);
	var sagemaker = new AWS.SageMaker({region: awsRegion});
	sagemaker.createEndpointConfig(endpointConfigurationParams, function(err, response){
		if(err){
			console.log(`Failed to create endpoint configuration: ${JSON.stringify(err)}`);
			callback(err);
		}
		else{
			console.log(`Successfully created endpoint configuration: ${JSON.stringify(response)}`);
			callback(null);
		}
	});
}

function getDescribeEndpointParams(){
	var endpointName = getEndpointName();
	var params = {
		EndpointName: endpointName
	};
	return params;
}

function checkForEndpoint(awsRegion, callback){
	var describeEndpointParams = getDescribeEndpointParams();
	var sagemaker = new AWS.SageMaker({region: awsRegion});
	sagemaker.describeEndpoint(describeEndpointParams, function(err, response){
		if(err){
			var errorString = JSON.stringify(err);
			if(errorString.indexOf("Could not find endpoint") > -1){
				console.log("No endpoint found. Will create new endpoint.");
				callback(null, null);
			}
			else{
				console.log(`Failed to describe endpoint: ${errorString}`);
				callback(err);
			}
		}
		else{
			console.log(`Found existing endpoint: ${JSON.stringify(response)}`);
			callback(null, response.EndpointName);
		}
	});
}

function getCreateUpdateEndpointParams(endpointConfigName){
	var endpointName = getEndpointName();
	var params = {
  		EndpointConfigName: endpointConfigName,
  		EndpointName: endpointName
	};
	return params;
}

function createEndpoint(awsRegion, dateString, callback){
	var endpointConfigName = getEndpointConfigurationName(dateString);
	var createUpdateEndpointParams = getCreateUpdateEndpointParams(endpointConfigName);
	console.log(`Creating endpoint with params: ${JSON.stringify(createUpdateEndpointParams)}`);
	var sagemaker = new AWS.SageMaker({region: awsRegion});
	sagemaker.createEndpoint(createUpdateEndpointParams, function(err, response){
		if(err){
			console.log(`Failed to create endpoint: ${JSON.stringify(err)}`);
			callback(err);
		}
		else{
			console.log(`Successfully created endpoint: ${JSON.stringify(response)}`);
			callback(null);
		}
	});
}

function updateEndpoint(awsRegion, dateString, callback){
	var endpointConfigName = getEndpointConfigurationName(dateString);
	var createUpdateEndpointParams = getCreateUpdateEndpointParams(endpointConfigName);
	console.log(`Updating endpoint with params: ${JSON.stringify(createUpdateEndpointParams)}`);
	var sagemaker = new AWS.SageMaker({region: awsRegion});
	sagemaker.updateEndpoint(createUpdateEndpointParams, function(err, response){
		if(err){
			console.log(`Failed to update endpoint: ${JSON.stringify(err)}`);
			callback(err);
		}
		else{
			console.log(`Successfully updated endpoint: ${JSON.stringify(response)}`);
			callback(null);
		}
	});
}