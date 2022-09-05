const fs = require('fs');
const parse = require('csv-parse').parse;

const inputFile = './candibell-dataset.csv';

console.log('*********** STARTING TO CLEAN DATASET ***********');

const writeStream = fs.createWriteStream('candibell-dataset-cleaned.csv');
writeStream.write(
  'id, timestamp, signalstrength, battery, temperature, consumptioncounter, buttoncounter, timezone\n'
);

const currentUser = {
  id: '',
  startDay: null,
  consumptionCount: null, // null stands for a value not yet set
};

fs.createReadStream(inputFile)
  .pipe(parse({ delimiter: ',' }))
  .on('data', function (csvrow) {
    const dataArray = [];
    for (dataPoint of csvrow) {
      if (dataPoint.length != 0) {
        const headerToValue = dataPoint.split('=');
        if (headerToValue.length === 2) {
          const header = headerToValue[0].trim();
          let value = headerToValue[1].trim();

          if (header === 'id' && currentUser.id !== value) {
            currentUser.id = value;
            currentUser.consumptionCount = null;
            currentUser.startDay = null;
          } else if (header === 'consumptioncounter') {
            if (currentUser.consumptionCount === value) {
              break;
            } else {
              currentUser.consumptionCount = value;
            }
          } else if (header === 'timestamp') {
            const currentDay = Number(value.split('-')[2].slice(0, 2));

            if (currentUser.startDay === null) {
              currentUser.startDay = Number(value.split('-')[2].slice(0, 2)); // naive method of just retriving the startDay of the month
            }

            value = currentDay - currentUser.startDay;
          }

          dataArray.push(value);
        }
      }
    }

    if (dataArray.length == 8) {
      writeStream.write(dataArray.join(', ') + '\n');
    }
  })
  .on('error', function (err) {
    console.log(' ********** ERROR **********');
    console.log(`Error message: ${err}`);
  })
  .on('end', function () {
    writeStream.end();
    console.log('*********** DATASET CLEANED ***********');
  });
