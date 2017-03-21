const path = require('path');
const fs = require('fs');
const logParser = require('./parser');
const $ = require('async');

var logsDir = path.resolve(process.cwd(), 'logs');

setTimeout(run, 5 * 1000); // Run the first time
setInterval(run, 1 * 60 * 60 * 1000); // Run every hour

// Read logs available and apply parser to each
// concurrently
function run() {
  process.nextTick(() => {
    var logfiles = fs.readdirSync(logsDir);
    console.log(`${logfiles.length} logs found`);
    if(!logfiles.length) { return console.log('No logs available'); }
    $.eachSeries(logfiles, parser, (err) => {
      console.log('done syncing records to DB');
    });
  });
}

function parser(logfile, cb) {
  const file = path.join(logsDir, logfile);
  logParser(file, cb);
}
