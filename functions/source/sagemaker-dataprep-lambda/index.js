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
	var dateString = new Date().toISOString().replace(":", "-").replace(":", "-").slice(0, -5);
	var hyperParameters = JSON.parse(process.env.HyperParameters);
	var contextLength = parseInt(hyperParameters.context_length);
	var predictionLength = parseInt(hyperParameters.prediction_length);
	var inputDataRows = inputS3Objects.map(function(o) { return JSON.parse(o); });
	var cats = inputDataRows.map(function(r) { return r.cat; });
	var minMaxCat = getMinMaxCat(cats, dateString);
	var minCat = minMaxCat["min"];
	var maxCat = minMaxCat["max"];
	for(var i = minCat; i < maxCat; i++) {
		if(cats.indexOf(i) < 0) {
			var blankRow = { "start": inputDataRows[1].start, "target": Array(contextLength+predictionLength).fill("0.000000"), "cat": i};
			console.log("Category %d is missing.  Adding blank row", i);
			inputDataRows.push(blankRow);
		}
	}
	for(var s3Object of inputDataRows){
		cleanAndSplitRow(s3Object, contextLength, predictionLength, minCat, trainingRows, testRows);
	}
	var sageMakerInputDataPrefix = process.env.SageMakerInputDataPrefix;
	var trainingDataKey = `${sageMakerInputDataPrefix}/${dateString}/train/train.json`;
	var testDataKey = `${sageMakerInputDataPrefix}/${dateString}/test/test.json`;
	console.log(`Writing ${trainingRows.length} training data rows to ${trainingDataKey}`);
	console.log(`Writing ${testRows.length} test data rows to ${testDataKey}`);
	
	var trainingData = sageMakerRowsToS3Object(trainingRows, trainingDataKey);
	var testData = sageMakerRowsToS3Object(testRows, testDataKey);
	var s3Objects = [trainingData, testData];
	return s3Objects;
}

function getMinMaxCat(cats, dateString) {
	var minCat = 9999999;
	var maxCat = -1;
	for(var category of cats){
		if(category < minCat) {
			minCat = category;
		}
		if(category > maxCat) {
			maxCat = category;
		}
	}
	var region = process.env.AWS_REGION;
	var bucket = process.env.InputBucketName;
	var prefix = process.env.InputPrefix;
	awsHelpers.sendDataToS3(region, bucket, `${prefix}/config.json`, `{"minCat":${minCat}}`, function(err) {
		if(err) console.log(err);
	});
	console.log(`Subtracting ${minCat} from every category to zero-index`);
	return {"min": minCat, "max": maxCat};
}

function sageMakerRowsToS3Object(rows, key){
	var s3Object = {
		key: key,
		data: rows.join('\n')
	};
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

function replicate(arr, times) {
     var al = arr.length,
         rl = al*Math.ceil(times),
         res = new Array(rl);
     for (var i=0; i<rl; i++)
         res[i] = arr[i % al];
     return res;
}

function cleanAndSplitRow(sageMakerRow, contextLength, predictionLength, minCat, trainingRows, testRows){
	if(sageMakerRow.target) {
		var requiredLength = contextLength + predictionLength;
		var cat = sageMakerRow.cat - minCat;
		if(sageMakerRow.target.length < requiredLength) {
			console.log("There were not enough items in the row for category", cat,
			"to generate a training and test file (Number of records:", sageMakerRow.target.length,
			"Required number:", requiredLength, ").  Repeating to get target length.");
			sageMakerRow.target = replicate(sageMakerRow.target, requiredLength/sageMakerRow.target.length)
		}
		var splitIndex = sageMakerRow.target.length - 1 - contextLength;
		var startTime = sageMakerRow.start.replace("Z", "");
		var targetItems = cleanRow(sageMakerRow.target);
		var trainingTarget = targetItems.slice(0, splitIndex);
		var testTarget = targetItems.slice(splitIndex);
		var trainingItem = new SageMakerRecord(startTime, trainingTarget, cat);
		//var trainStartTime = new Date(Date.parse(startTime).getTime() + splitIndex * 3600000);
		var testItem = new SageMakerRecord(startTime, testTarget, cat);
		trainingRows.push(JSON.stringify(trainingItem));
		testRows.push(JSON.stringify(testItem));
	} else {
		console.log("Invalid row: ", sageMakerRow);
	}
}
