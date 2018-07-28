"use strict";

module.exports = class PriceHistoryRecord {

    constructor (availabilityZone, instanceType, spotPrice, timestamp, category) {
        this.availabilityZone = availabilityZone;
        this.instanceType = instanceType;
        this.spotPrice = spotPrice;
        this.timestamp = timestamp;
        this.category = category;
    }
}