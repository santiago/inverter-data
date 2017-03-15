const path = require('path');
const fs = require('fs');
const logParser = require('./parser');
const $ = require('async');

var logsDir = path.resolve(process.cwd(), './logs');
// Read logs available and apply parser to each
// concurrently
// $.each(fs.readdirSync(logsDir), parser, (err) => {
//   console.log('done syncing records to DB');
// });

function parser(logfile, cb) {
  const file = path.join(logsDir, logfile)
  logParser(file, cb);
}
