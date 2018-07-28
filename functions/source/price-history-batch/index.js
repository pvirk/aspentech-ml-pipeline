"use strict";

module.exports = class PriceHistoryBatch {

    constructor (startTime, endTime, records) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.records = records;
    }
}