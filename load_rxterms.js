// Loads the data into the database.
// Run "start_es.sh" first to start the database.
// Then run this with "node load_rxterms.js".
var indexName = "rxterms";
var indexBuilder = require('./indexBuilder.js');
indexBuilder.refreshIndex(indexName, loadData);


// All the code after this line is used for the loadData function
/**
 * Loads the data. This gets called after the mapping is created.
 * @param esClient the elasticsearch client. It needs to be closed when the
 * loadData function is completed
 */
function loadData(esClient) {
  var fs = require('fs');
  var readline = require('readline');
  var dataFileConf = require('../server/indexConf/' + indexName + '.conf.js').dataFile;
  var dataFile = dataFileConf.name;
  var delimiter = dataFileConf.fieldDelimiter;

  // Load the rxterms data
  var rd = readline.createInterface({
    input: fs.createReadStream(dataFile),
    output: process.stdout,
    terminal: false
  });

  // Variables to be passed to streaming closing function
  var maxDigitNumLookup = {};
  var displayNameToDocs = {};

  // Variables used by streaming reading only
  var mixedList = [], headers, headerToIndex = {};

  rd.on('line', function (line) {
    var fields = line.split(delimiter);
    if (headers === undefined) {
      headers = fields;
      headerToIndex = {};
      for (var i = 0, max = headers.length; i < max; i++) {
        headerToIndex[headers[i]] = i;
      }
    }
    else {
      var displayName = fields[headerToIndex["DISPLAY_NAME"]];
      var isRetired = fields[headerToIndex["IS_RETIRED"]].trim() !== '';
      var isSuppressed = fields[headerToIndex["SUPPRESS_FOR"]].trim() !== '';

      if (!displayName)
        throw 'Error on: ' + line + '(missing display name).';

      // Build the hash (i.e. maxDigitNumLookup) from displayName to maximum
      // number of digits from strength field only for the non-retired records.
      // Also update the digitNum variable.
      // Later on, we will use digitNum and maximum number of digits to pad the
      // new 'text' field.
      if (!isRetired && !isSuppressed) {
        if (!displayNameToDocs[displayName])
          displayNameToDocs[displayName] = [];

        var strength = fields[headerToIndex["STRENGTH"]];
        var newDoseForm = fields[headerToIndex["NEW_DOSE_FORM"]];
        var rxcui = fields[headerToIndex["RXCUI"]];

        var numStr = '', digitNum = null;
        // Skip the mixed records if found one (identified by its display name)
        // already.
        if (mixedList.indexOf(displayName) === -1) {
          // No padding for mixed records
          if (strength === "mixed") {
            maxDigitNumLookup[displayName] = 0;
            mixedList.push(displayName);
          }
          else {
            // Find and set the maximum digit number for records with non-mixed
            // strength
            numStr = (/^\s*([\d,]+)/).exec(strength);
            numStr = numStr && numStr[1];
            if (numStr) {
              digitNum = numStr.length;
              var maxNum = maxDigitNumLookup[displayName];
              if (!maxNum || (maxNum < digitNum))
                maxDigitNumLookup[displayName] = digitNum;
            }
            else {
              throw ("Bad strength data for rxcui " + rxcui);
            }
          }
        }
        displayNameToDocs[displayName].push({
          STRENGTH: strength,
          DISPLAY_NAME_SYNONYM: fields[headerToIndex['DISPLAY_NAME_SYNONYM']],
          NEW_DOSE_FORM: newDoseForm,
          DIGIT_NUM: digitNum,
          RXCUI: rxcui
        });
      }
    }
  });

  rd.on('close', function () { // when the underlying stream is closed
    rd.close(); // also can emit a close event, but doesn't here
    var dataToStore = buildRxTermsIndexDocuments(displayNameToDocs,
      maxDigitNumLookup);

    // Now that we have all the data, store it
    indexBuilder.storeIndexedData(indexName, dataToStore, esClient);
  });
}


/**
 * Generate strength-and-form text using the document in displayNameToDocs and
 * add proper padding based on the maximum number of digits found in
 * maxDigitNumLookup. Return an array of documents with the following fields:
 * 1) DISPLAY_NAME: a display name
 * 2) STRENGTHS_AND_FORMS: a list of strength-and-form texts
 *
 * @param displayNameToDocs a hash from display name to a list of associated
 * documents
 * @param maxDigitNumLookup a hash from a display name to the maximum number
 * of digits found in the list of associated strengths
 */
function buildRxTermsIndexDocuments(displayNameToDocs, maxDigitNumLookup) {
  var rtn = [];
  for (var displayName in displayNameToDocs) {
    // Generate list of strength_and_form texts for the current display name
    var strengthAndFormList = [];
    var docs = displayNameToDocs[displayName];
    var strengthToRxcui = {}; // for sorting later
    var synonyms = {};
    for (var i = 0, max = docs.length; i < max; i++) {
      var doc = docs[i];
      var strength = doc['STRENGTH'];
      var digitNum = doc['DIGIT_NUM'];
      var newDoseForm = doc['NEW_DOSE_FORM'];
      var maxDigitNum = maxDigitNumLookup[displayName];
      var synonym = doc['DISPLAY_NAME_SYNONYM'];
      if (synonym)
        synonyms[synonym] = 1;

      // Generate a strength-and-form text and store it into strengthAndFormList
      if (maxDigitNum !== undefined) {
        var text = '';
        if (maxDigitNum > 0) {
          // Add padding
          for (var j = maxDigitNum; j > digitNum; j--)
            text += ' ';
          text += strength.trim() + ' ' + newDoseForm;
        }
        else {
          text = strength + ' ' + newDoseForm;
        }
        if (!strengthToRxcui[text]) {
          strengthAndFormList.push(text);
          strengthToRxcui[text] = doc['RXCUI'];
        }
        else {
          // We have a duplicate strength-form text.  Use the lower RXCUI value.
          if (parseInt(doc['RXCUI']) < parseInt(strengthToRxcui[text]))
            strengthToRxcui[text] = doc['RXCUI'];
        }
      }
    }

    var newLine = {};
    newLine['DISPLAY_NAME'] = displayName;
    newLine['DISPLAY_NAME_SYNONYM'] = Object.keys(synonyms);
    newLine["STRENGTHS_AND_FORMS"] = strengthAndFormList.sort();
    // Now create a list of RXCUIs that follows the sorted strength list
    var rxcuis = [];
    for (i=0, len=strengthAndFormList.length; i<len; ++i)
      rxcuis.push(strengthToRxcui[strengthAndFormList[i]]);
    newLine['RXCUIS'] = rxcuis;
    rtn.push(newLine);
  }
  return rtn;
}
