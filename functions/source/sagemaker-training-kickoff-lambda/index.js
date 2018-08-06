var AWS = require('aws-sdk');
var async = require('async');
var awsHelpers = require('aws-helpers');

exports.handler = (event, context, callback) => {
	var awsRegion = process.env.AWS_REGION;
	console.log('Received event:', JSON.stringify(event, null, 2));
    async.each(event.Records, function(record, callback){
		kickoffTrainingJob(awsRegion, record, callback);
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

function kickoffTrainingJob(awsRegion, record, callback){
	getParams(awsRegion, record, function(err, params){
		if(err){
			callback(err);
		}
		else{
			var sagemaker = new AWS.SageMaker({region: awsRegion});
			sagemaker.createTrainingJob(params, function(err, response) {
		  		if (err){
		  			console.log(`Failed to create training job: ${err}`);
		  			callback(err);
		  		}
		  		else{
		  			console.log(`Successfully created training job: ${JSON.stringify(response)}`);
		  			callback(null);
		  		}
			});
		}
	});
}

function getNumCats(awsRegion, s3Bucket, s3Key, callback){
	awsHelpers.readS3Object(awsRegion, s3Bucket, s3Key, function(err, object){
		if(err){
			console.log(`Failed to read object from S3: ${err}`);
			callback(err);
		}
		else{
			var numLines = object.split(/\r\n|\r|\n/).length.toString();
			callback(null, numLines);	
		}
	});
}

function getParams(awsRegion, record, callback){
	var s3Bucket = record.s3.bucket.name;
	var trainingKey = record.s3.object.key.replace("train.json", "");
	var testKey = trainingKey.replace("train/", "test/");
	var hyperParameters = JSON.parse(process.env.HyperParameters);
	var dateString = new Date().toISOString().replace(":", "-").replace(":", "-").slice(0, -5);
	var instanceCount = parseInt(process.env.TrainingInstanceCount);
	var instanceType = process.env.TrainingInstanceType;
	var instanceVolumeSize = parseInt(process.env.TrainingInstanceVolumeSize);
	var inputDataConfig = getInputDataConfig(s3Bucket, trainingKey, testKey);
	var outputBucket = process.env.OutputBucketName;
	var outputPrefix = process.env.SageMakerOutputModelPrefix;
	var outputDataConfig = getOutputDataConfig(outputBucket, outputPrefix, dateString);
	var resourceConfig = getResourceConfig(instanceCount, instanceType, instanceVolumeSize);
	var trainingJobName = getTrainingJobName(dateString);
	var roleArn = process.env.TrainingRoleArn;
	var trainingImage = process.env.TrainingImage;
	var algorithmSpecification = getAlgorithmSpecification(trainingImage);
	var maxRuntimeSeconds = process.env.TrainingMaxRuntimeSeconds;
	var stoppingCondition = getStoppingCondition(maxRuntimeSeconds);
	var params = {
  		AlgorithmSpecification: algorithmSpecification,
		InputDataConfig: inputDataConfig,
		OutputDataConfig: outputDataConfig,
		ResourceConfig: resourceConfig,
		RoleArn: roleArn,
		StoppingCondition: stoppingCondition,
		TrainingJobName: trainingJobName,
		HyperParameters: hyperParameters
	};
	callback(null,  params);
}


function getInputDataConfig(bucketName, trainingDataKey, testDataKey){
	var trainingDataUri = `s3://${bucketName}/${trainingDataKey}`;
	var testDataUri = `s3://${bucketName}/${testDataKey}`;
	var channels = [
	    {
	      ChannelName: 'train',
	      DataSource: {
	        S3DataSource: {
	          S3DataType: 'S3Prefix',
	          S3Uri: trainingDataUri
	        }
	      },
	      ContentType: 'json'
	    },
	    {
	      ChannelName: 'test',
	      DataSource: {
	        S3DataSource: {
	          S3DataType: 'S3Prefix',
	          S3Uri: testDataUri
	        }
	      },
	      ContentType: 'json'
	    }
  	];
  	return channels;
}

function getOutputDataConfig(outputBucket, outputPrefix, dateString){
	var outputDataUri = `s3://${outputBucket}/${outputPrefix}/${dateString}`;
	var outputDataConfig = {
    	S3OutputPath: outputDataUri
  	};
  	return outputDataConfig;
}

function getResourceConfig(instanceCount, instanceType, instanceVolumeSize){
	var resourceConfig = {
    	InstanceCount: instanceCount,
    	InstanceType: instanceType,
    	VolumeSizeInGB: instanceVolumeSize
  	};
  	return resourceConfig;
}

function getTrainingJobName(dateString){
	return `training-${dateString}`;
}
	
function getAlgorithmSpecification(trainingImage){
	var algorithmSpecification = {
		TrainingImage: trainingImage,
    	TrainingInputMode: 'File'
	};
    return algorithmSpecification;
}

function getStoppingCondition(maxRuntimeSeconds){
	var stoppingCondition = {
		MaxRuntimeInSeconds: maxRuntimeSeconds
	};
	return stoppingCondition;
}