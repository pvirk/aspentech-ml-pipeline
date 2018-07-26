var AWS = require('aws-sdk');
var async = require('async');
var awsHelpers = require('aws-helpers');
var SageMakerRecord = require('sagemaker-record');

exports.handler = (event, context, callback) => {
	console.log('Triggered by CloudWatch');
	var inputBucketName = process.env.InputBucketName;
	var inputPrefix = process.env.InputPrefix;
	var outputBucketName = process.env.OutputBucketName;
	var awsRegion = process.env.AWS_REGION;
	prepData(awsRegion, inputBucketName, outputBucketName, inputPrefix, function(err){
		if(err) {
			var message = `Failed to process CloudWatch event: ${err}`;
			console.log(message);
			context.fail(message);
		}
		else {
			var message = "Successfully processed CloudWatch event";
			console.log(message);
			context.succeed(message);
		}
	});
};

function prepData(awsRegion, inputBucketName, outputBucketName, inputPrefix, outerCallback){
    console.log('Reading data from Region: %s:, Bucket: %s, Prefix: %s.', awsRegion, inputBucketName, inputPrefix);
    awsHelpers.listAllS3Objects(awsRegion, inputBucketName, inputPrefix, [], null, function(allS3Objects){
        awsHelpers.readAllS3Objects(awsRegion, inputBucketName, allS3Objects, function(err, objects){
			if(err){
				var message = `Failed to read data from S3: ${err}`
				console.log(message);
				outerCallback(err);
			}
			else{
				console.log(`Found ${objects.length} objects in S3`);
				var objectsToWrite = translateData(objects);
				async.each(objectsToWrite, function(s3Object, innerCallback){
					awsHelpers.sendDataToS3(awsRegion, outputBucketName, s3Object.key, s3Object.data, innerCallback);
				}, function(err){
					if(err){
						outerCallback(err);
					}
					else{
						outerCallback(null);
					}
				});
			}
		});
    })
}

function translateData(inputS3Objects){
	var trainingRows = [];
	var testRows = [];
	var hyperParameters = JSON.parse(process.env.HyperParameters);
	var contextLength = parseInt(hyperParameters.context_length);
	var predictionLength = parseInt(hyperParameters.prediction_length);
	for(var s3Object of inputS3Objects){
		cleanAndSplitRow(s3Object, contextLength, predictionLength, trainingRows, testRows);
	}
	var sageMakerInputDataPrefix = process.env.SageMakerInputDataPrefix;
	var dateString = new Date().toISOString().replace(":", "-").replace(":", "-").slice(0, -5);
	var trainingDataKey = `${sageMakerInputDataPrefix}/${dateString}/train/train.json`;
	var testDataKey = `${sageMakerInputDataPrefix}/${dateString}/test/test.json`;
	console.log(`Writing training data to ${trainingDataKey}`);
	console.log(`Writing test data to ${testDataKey}`);
	var trainingData = sageMakerRowsToS3Object(trainingRows, trainingDataKey);
	var testData = sageMakerRowsToS3Object(testRows, testDataKey);
	var s3Objects = [trainingData, testData];
	return s3Objects;
}

function sageMakerRowsToS3Object(rows, key){
	var s3Object = {
		key: key,
		data: rows.join('\n')
	}
	return s3Object;
}

function cleanRow(targetItems){
	var previousItem = "0.0";
	for(var i = 0; i < targetItems.length; i++){
		if(targetItems[i] == null){
			targetItems[i] = previousItem;
		}
		else{
			previousItem = targetItems[i];
		}
	}
	return targetItems;
}

function cleanAndSplitRow(s3Object, contextLength, predictionLength, trainingRows, testRows){
	var sageMakerRow = JSON.parse(s3Object);
	if(sageMakerRow.target.length > (contextLength + predictionLength)){
		var splitIndex = sageMakerRow.target.length - 1 - contextLength;
		var trainingItem = {};
		var testItem = {};
		var startTime = sageMakerRow.start;
		var cat = sageMakerRow.cat;
		var targetItems = cleanRow(sageMakerRow.target);
		var trainingTarget = targetItems.slice(0, splitIndex);
		var testTarget = targetItems.slice(splitIndex);
		var trainingItem = new SageMakerRecord(startTime, trainingTarget, cat);
		var testItem = new SageMakerRecord(startTime, testTarget, cat);
		trainingRows.push(JSON.stringify(trainingItem));
		testRows.push(JSON.stringify(testItem));
	} else {
		console.log("There were not enough items in the row for category", sageMakerRow.cat,
		"to generate a training and test file (Number of records:", sageMakerRow.target.length,
		"Required number:", contextLength + predictionLength, ").");
	}
}
