const fs = require('fs');
const path = require('path');
const $ = require('async');
const mysql = require('mysql');

const sendEmail = require('./email');

const headers = [ 'Interval', 'DateTime', 'Serial', 'P_AC', 'E_DAY', 'T_WR', 'U_AC', 'U_AC_1', 'U_AC_2', 'U_AC_3', 'I_AC', 'F_AC', 'U_DC_1', 'I_DC_1', 'U_DC_2', 'I_DC_2', 'U_DC_3', 'I_DC_3', 'S', 'E_WR', 'M_MR', 'I_AC_1', 'I_AC_2', 'I_AC_3', 'P_AC_1', 'P_AC_2', 'P_AC_3', 'F_AC_1', 'F_AC_2', 'F_AC_3', 'R_DC', 'PC', 'PCS', 'PCS_LL', 'COS_PHI', 'COS_PHI_LL', 'S_COS_PHI', 'Current_Day_Energy', 'Current_Day_Offset', 'ccEnergyOfDay_WithoutOffset' ];
const E_WR = headers.indexOf('E_WR');
const DateTime = headers.indexOf('DateTime');
const Serial = headers.indexOf('Serial');
const Current_Day_Energy = headers.indexOf('Current_Day_Energy');

var registry = {};
var daylights = {};

loadErrorsFromDB((err) => {
  // console.log(errors);
});

module.exports = logParser;

function logParser(logfile, done) {
  const self = this;

  const _done = (err)=> {
    console.log(err);
    // It's done parsing the `logfile`.
    // Close the DB handle.
    self.db.destroy();
    // Archive the parsed logfile
    archiveLog(logfile, ()=> { done(err) });
  }

  fs.readFile(logfile, 'ascii', (err, data) => {
    if(err) {
      console.log("Error reading file "+logfile);
      console.log("Most likely it is a directory");
      // console.log(err);
      return done();
    }

    self.db = getDb();

    var isLog = false;
    var isHeader = false;

    const lines = data.split('\n').filter(l => {
      if(isHeader) { isHeader = false; isLog = true; return; }
      if(l.match(/^\[wr\]$/)) { isHeader = true }
      if(l.match(/^\[wr_end/)) { isLog = false }
      return l && isLog;
    });

    // Read each line and save to DB
    $.eachSeries(lines, processRecord.bind(this), _done);
  });
}

function processRecord(r, cb) {
  var values = r.split(';');

  // If the value in the E_WR is different than zero, then
  // notify the error via email
  if(parseInt(values[E_WR])) { sendErrorEmail(values[E_WR]); }

  const serial = values[Serial];
  const time = (new Date(values[DateTime])).getTime();
  const currentEnergy = parseInt(values[Current_Day_Energy]);

  // If processed record is still not in registry, add it
  registry[serial] || (registry[serial] = { time, currentEnergy });

  // Parse the record and save to DB, then update Devices table
  $.applyEachSeries([
    updateInverterData.bind(this),
    updateDevices.bind(this),
    notifyProduction.bind(this)
  ], values, cb);
  // setTimeout(cb, 10);
}

function notifyProduction(values, done) {
  // If time lies in daylight check
  // whether there was power collected during
  // the last hour, otherwise send an email
  // notifying possible malfunction
  const date = new Date(values[DateTime]);
  const time = date.getTime();
  const serial = values[Serial];
  const currentEnergy = parseInt(values[Current_Day_Energy]);

  checkDaylight(date, time, (err, isDaylight) => {
    if(err) { return done(err) }
    if(!isDaylight) { return done() }

    // Only if it's daylight
    const lastTime = registry[serial].time;
    // Check every 1 hour
    if(time - lastTime > 1 * 60 * 60 * 1000) {
      const lastEnergy = registry[serial].currentEnergy;
      // If panel collected no power during this
      // time, notify via email
      console.log('currentEnergy', currentEnergy);
      console.log('lastEnergy', lastEnergy);
      if(!(currentEnergy > lastEnergy)) {
        sendErrorEmail('999');
      }
      registry[serial] = { time, currentEnergy };
    }

    done();
  });
}

function updateDevices(values, done) {
  const updates = [
    `UPDATE devices SET LastSeen = "${values[DateTime]}" WHERE Serial = "${values[Serial]}";`,
    `UPDATE devices SET LastProduction = ${values[Current_Day_Energy]} WHERE Serial = "${values[Serial]}";`,
    `UPDATE devices SET CurrentError = ${values[E_WR]} WHERE Serial = "${values[Serial]}";`
  ];

  var db = this.db;
  $.eachSeries(updates, (q, cb) => {
    db.query(q, (err, results) => {
      if(err) { console.log(`Error updating Devices tables. ${err}`); return cb(err); }
      cb();
    });
  }, done);
}

function updateInverterData(values, cb) {
  var data = values.slice();
  // Second column is a date time string so we need to quote it
  data.splice(1, 1, `"${values[DateTime]}"`);
  // Third column `Serial` is a string code so we need to quote it
  data.splice(2, 1, `"${values[Serial]}"`);
  const insert = `INSERT INTO inverterdata (\`${headers.join('\`,\`')}\`, ID)
    VALUES(${data.join(',')}, NULL);`;

  this.db.query(insert, (err) => {
    if(err) { console.log(err); return cb(); }
    cb();
  });
}

// Manage error in solar panel
var errors = {};

function loadErrorsFromDB(cb) {
  const db = getDb();

  db.query('SELECT ID, Description FROM errorcodes', (err, results) => {
    if(err) { console.log(err); return cb(err); }
    results.forEach((r) => {
      errors[r.ID] = r.Description;
    });
    errors['999'] = 'Has not collected power in the last hour.';

    db.destroy();
    cb();
  });
}

function checkDaylight(date, time, cb) {
  var day = toDayFormat(date);

  // Check if there's daylight already
  // for the given day in format "DD/MM/YYYY" ...
  if(daylights[day]) {
    console.log('sunset', daylights[day].sunset);
    console.log('time', time);
    const isDaylight = daylights[day].sunset > time;

    // To keep things async we invoke callback
    // in the next iteration of the Event Loop
    return process.nextTick(() => {
      console.log("isDaylight", isDaylight);
      cb(null, isDaylight);
    });
  }

  // ... otherwise grab from DB
  const db = getDb();

  const query = `SELECT Dawn, Sunrise, Sunset FROM daylighthours
    WHERE \`Date\` = '${day}'`;

  db.query(query, (err, results) => {
    if(err) {
      console.log('Error getting daylight hours');
      console.log(err);
      return cb(err);
    }

    var sunrise = '8:00';
    var sunset = '17:00';

    if(results && results.length) {
      var r = results.pop();
      sunrise = r.Sunrise;
      sunset = r.Sunset;
    }

    const sunriseHours = parseInt(sunrise.split(':').shift());
    const sunriseMinutes = parseInt(sunrise.split(':').pop());
    var sunriseDate = new Date(time);
    sunriseDate.setHours(sunriseHours);
    sunriseDate.setMinutes(sunriseMinutes);

    const sunsetHours = parseInt(sunset.split(':').shift());
    const sunsetMinutes = parseInt(sunset.split(':').pop());
    var sunsetDate = new Date(time);
    sunsetDate.setHours(sunsetHours);
    sunsetDate.setMinutes(sunsetMinutes);

    daylights[day] = { sunrise: sunriseDate.getTime(), sunset: sunsetDate.getTime() };

    db.destroy();

    checkDaylight(date, time, cb)
  });
}

/*
 * archiveLog: send the parsed log to an archive directory
 */
function archiveLog(logfile, cb) {
  const file = logfile.split('/').pop();
  const archiveDir = path.resolve(process.cwd(), 'archive');
  const newPath = path.join(archiveDir, file);
  fs.link(logfile, newPath, () => {
    fs.unlinkSync(logfile);
    cb();
  });
}

function getDb() {
  return mysql.createConnection({
    host     : 'localhost',
    user     : 'shopkins_backend',
    password : 'Nnd3l9&7',
    database : 'shopkins_backend'
  });
}

function toDayFormat(date) {
  return date.toISOString().split('T')[0].split('-').reverse().join('/');
}

function sendErrorEmail(errorCode) {
  const errorEmailFrom = 'sender@address';
  const errorEmailTo = 'receiver@address';
  const errorEmailSubject = 'An error occurred in panel operation #PANEL_ID';

  const code = parseInt(errorCode);
  if(code) {
    var err = errors[errorCode];
    sendEmail({
       from: errorEmailFrom,
       to: errorEmailFrom,
       subject: errorEmailSubject,
       html: `<b>${err}</b>`,
       text: err
    });

    console.log('sendErrorEmail', err);
  }
}
