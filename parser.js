const fs = require('fs');
const path = require('path');
const $ = require('async');
const mysql = require('mysql');

const headers = [ 'Interval', 'DateTime', 'Serial', 'P_AC', 'E_DAY', 'T_WR', 'U_AC', 'U_AC_1', 'U_AC_2', 'U_AC_3', 'I_AC', 'F_AC', 'U_DC_1', 'I_DC_1', 'U_DC_2', 'I_DC_2', 'U_DC_3', 'I_DC_3', 'S', 'E_WR', 'M_WR', 'I_AC_1', 'I_AC_2', 'I_AC_3', 'P_AC_1', 'P_AC_2', 'P_AC_3', 'F_AC_1', 'F_AC_2', 'F_AC_3', 'R_DC', 'PC', 'PCS', 'PCS_LL', 'COS_PHI', 'COS_PHI_LL', 'S_COS_PHI', 'Current_Day_Energy', 'current_Day_Offset', 'ccEnergyOfDay_WithoutOffset' ];
const E_WR = headers.indexOf('E_WR');
const DateTime = headers.indexOf('Interval');
const Serial = headers.indexOf('Serial');

var db = mysql.createConnection({
  host     : '107.170.65.218', // localhost',
  user     : 'shopkins_backend',
  password : 'Nnd3l9&7',
  database : 'shopkins_backend'
});

loadErrors((err) => {
  console.log(err);
});

module.exports = logParser;

function logParser(logfile, done) {
  var isLog = false;
  var isHeader = false;

  fs.readFile(logfile, 'utf8', (err, data) => {
    const lines = data.split('\n');
    // Read each line concurrently and save to DB
    $.each(lines, (l, cb) => {
      if(isHeader) { isHeader = false; isLog = true; return; }
      if(l.match(/^\[wr\]$/)) { isHeader = true; return; }
      if(l.match(/^\[wr_end/)) { return; }
      l && isLog && parseAndSave(l, cb);
    }, done);
  });
}

function parseAndSave(r, cb) {
  var values = r.split(';');
  // If the value in the
  if(parseInt(values[E_WR])) { sendErrorEmail(values[errorIndex]); }

  // Second column is a date time string so we need to quote it
  values.splice(1, 1, `"${values[DateTime]}"`);
  // Third column `Serial` is a code so we need to quote it
  values.splice(2, 1, `"${values[Serial]}"`);
  const insert = `INSERT INTO "InverterData" ("${headers.join('","')}", "ID")
    VALUES(${values.join(',')}, NULL);`;

  db.query(insert, (err) => {
    if(err) { console.log(err); return cb(); }
    cb();
  });
}

// Manage error in panel
var errors = {};

function loadErrors(cb) {
  db.query('SELECT ID, Description FROM ErrorCodes', (err, results) => {
    if(err) { console.log(err); return cb(err); }
    results.forEach((r) => {
      errors[r.ID] = r.Description;
      console.log(`${r.ID} - ${r.Description}`);
    });
  });
}

// Email facilities
const nodemailer = require('nodemailer');
// const transporter = nodemailer.createTransport();

const errorEmailFrom = 'sender@address';
const errorEmailTo = 'receiver@address';
const errorEmailSubject = 'An error occurred in panel operation #PANEL_ID';

function sendErrorEmail(error) {
  console.log(error);
  /*transporter.sendMail({
     from: errorEmailFrom,
     to: errorEmailFrom,
     subject: errorEmailSubject,
     html: `<b>${errorEmailSubject}</b>`,
     text: errorEmailSubject
  });*/
}
