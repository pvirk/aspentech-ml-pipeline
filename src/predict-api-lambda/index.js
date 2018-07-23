var async = require('async');
var awsHelpers = require('aws-helpers');
var AWS = require('aws-sdk');
var oneWeek = 60*60*1000*24*7;

exports.handler = (event, context, callback) => {
	console.log('Received event:', JSON.stringify(event, null, 2));
    var startDate = new Date(Date.parse(event.startDate));
    var endDate = new Date(Date.parse(event.endDate));
    var instanceType = event.instanceType;
    var availabilityZone = event.availabilityZone;
    var validationResult = validate(startDate, endDate, callback);
    if(validationResult) {
        callback(validationResult, null);
    } else {
        console.log('Predicting for data between %s and %s', startDate.toISOString(), endDate.toISOString());
        var historicalEnd = new Date();
        var historicalStart = new Date(historicalEnd.getTime() - oneWeek);
        console.log('Getting historical data between %s and %s', historicalStart.toISOString(), historicalEnd.toISOString());
        readData(instanceType, availabilityZone, function(err, results) {
            if(err) { callback(err, null); }
            else {
                var instanceTypePrices = JSON.parse(results);
                executeSageMakerDeepAR(process.env.SageMakerEndpointName, instanceTypePrices, startDate, endDate, callback);
            }
        });
    }
};

function validate(startDate, endDate, callback) {
    if(startDate > endDate) {
        return "Start Date must be before End Date";
    } else if(startDate.getTime > new Date().getTime) {
        return "Start Date must be in the future";
    } else if(endDate > new Date().getTime + 1000*60*60*24) {
        return "End Date must be less than 1 day in the future";
    }
    return null;
}


function readData(instanceType, az, callback)
{
    var awsRegion = process.env.AWS_REGION;
    var bucketName = process.env.OutputBucketName;
    var prefix = process.env.ObjectPrefix;
    var key = prefix+"/"+az+"/"+instanceType+".json";
    console.log("getting data from file s3://%s/%s", bucketName, key);
    awsHelpers.readS3Object(awsRegion, bucketName, key, callback);
}

function executeSageMakerDeepAR(endpointName, historicals, startDate, endDate, callback) {
    var oneHour = 1000*60*60;
    var sagemakerruntime = new AWS.SageMakerRuntime();
    var now = new Date();
    var hoursOut = Math.ceil((endDate - now) / oneHour);
    var hourSpan = Math.ceil((endDate.getTime() - startDate.getTime()) / oneHour);
    var target = [];
    for(var i = 0; i < historicals.target.length; i++) {
        target[i] = parseFloat(historicals.target[i]);
    }
    historicals.target = target;
    var deepARBody = { 
        "instances": [historicals],
        "configuration": {
            "num_samples": 100,
            "output_types": ["mean", "quantiles"],      
            "quantiles": ["0.1", "0.9"]
        }

    };
    var body = JSON.stringify(deepARBody);
    var params = {
        Body: body,
        EndpointName: endpointName,
        Accept: 'application/json',
        ContentType: 'application/json'
    };
    console.log("executing DeepAR endpoint %s with body %s", endpointName, body);
    sagemakerruntime.invokeEndpoint(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else {
            var body = data.Body.toString();
            console.log("SageMaker predictions: ", body);
            var predictions = JSON.parse(body);
            var sageMakerResults = predictions["predictions"][0];
            var meanValues = sageMakerResults["mean"].slice(hoursOut-hourSpan,hoursOut+1);
            var lowerValues = sageMakerResults["quantiles"]["0.1"].slice(hoursOut-hourSpan,hoursOut+1);
            var upperValues = sageMakerResults["quantiles"]["0.9"].slice(hoursOut-hourSpan,hoursOut+1);
            var results = [];
            for(var i = 0; i < meanValues.length; i++) {
                results[i] = {
                    "when": new Date(startDate.getTime()+i*oneHour).toISOString(), 
                    "mean": meanValues[i], 
                    "tenPercent": lowerValues[i], 
                    "ninetyPercent": upperValues[i] 
                };
            }   
            console.log("results: %s", JSON.stringify(results));
            callback(null, results);
        }    
    });
  

}