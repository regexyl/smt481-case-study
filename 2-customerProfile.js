const fs = require('fs');
const parse = require('csv-parse').parse;

const inputFile = './candibell-dataset-cleaned.csv';
const writeStream = fs.createWriteStream('candibell-dataset-profiles.csv');

const currentUser = {
  id: null,
  previousConsumptionDay: null,
  previousConsumptionCount: null,
  consumptionHistory: [],
};

const indices = {
  id: 0,
  day: 1,
  consumptionCounter: 5,
};

fs.createReadStream(inputFile)
  .pipe(parse({ delimiter: ',', from_line: 2 }))
  .on('data', function (csvrow) {
    const id = csvrow[indices.id];
    const day = csvrow[indices.day];
    const consumptionCounter = Number(
      csvrow[indices.consumptionCounter].trim()
    );

    if (id !== currentUser.id) {
      writeStream.write(currentUser.consumptionHistory.join(', ') + '\n');
      currentUser.id = id;
      currentUser.previousConsumptionDay = null;
      currentUser.previousConsumptionCount = null;
      currentUser.consumptionHistory = [];
    }

    const dayDifferenceFromLastConsumption =
      day - currentUser.previousConsumptionDay;
    if (dayDifferenceFromLastConsumption > 1) {
      for (let i = 0; i < dayDifferenceFromLastConsumption - 1; i++) {
        currentUser.consumptionHistory.push(
          currentUser.previousConsumptionCount
        );
      }
    }

    currentUser.previousConsumptionDay = day;
    currentUser.previousConsumptionCount = consumptionCounter;
    currentUser.consumptionHistory.push(consumptionCounter);
  });
