'use strict';

const { Readable } = require('stream');

const DEFAULT_BATCH_SIZE = 100;


/**
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {number} batchSize - size of batches to fetch from database
 * @param {number} limit - Sequelize limit parameter
 * @param {number} offset - Sequelize offset parameter
 * @param {object} params - other Sequelize parameters
 */
async function performSearch(
    model,
    inputStream,
    { batchSize = DEFAULT_BATCH_SIZE, limit, offset = 0, ...params }
) {
  let max = limit;
  if (!max) {
    max = await model.count({ ...params, limit, offset });
  }
  const offsets = [];
  let start = offset;
  while (start < max) {
    offsets.push(start);
    start += batchSize;
  }
  for (const offset of offsets) {
    const difference = (batchSize + offset - max);
    const items = await model.findAll({
      ...params,
      offset,
      limit: difference > 0 ? batchSize - difference : batchSize,
    });
    inputStream.push(JSON.stringify(items));
  }
  inputStream.push(null); // Means end of stream
}


/**
 * @param {object} params - Sequelize parameters
 * @return {object} - readable stream object
 */
function findAllWithStream(params) {
  const model = this;
  const inputStream = new Readable({
    read: function() {},
  });
  performSearch(model, inputStream, { batchSize: model.BATCH_SIZE, ...params });
  return inputStream;
}


/**
 * @param {object} sequelize - Sequelize object
 * @param {number} defaultBatchSize - default batch size
 */
function init(sequelize, defaultBatchSize) {
  for (const modelName of Object.keys(sequelize.models)) {
    sequelize.models[modelName].findAllWithStream = findAllWithStream;
    if (!sequelize.models[modelName].BATCH_SIZE || defaultBatchSize) {
      sequelize.models[modelName].BATCH_SIZE = defaultBatchSize || DEFAULT_BATCH_SIZE;
    }
  }
}


module.exports = init;
