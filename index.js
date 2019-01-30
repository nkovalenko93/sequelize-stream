'use strict';

const { Readable } = require('stream');
const lodash = require('lodash');

const DEFAULT_BATCH_SIZE = 100;


/**
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {number} batchSize - size of batches to fetch from database
 * @param {number} limit - Sequelize limit parameter
 * @param {number} offset - Sequelize offset parameter
 * @param {object} params - other Sequelize parameters
 */
async function performSearch(model, inputStream, { batchSize = DEFAULT_BATCH_SIZE, limit, offset = 0, ...params }) {
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
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {array} items - items to be created
 * @param {number} batchSize - size of batches to fetch from database
 */
async function performBulkCreate(model, inputStream, items, { batchSize = DEFAULT_BATCH_SIZE }) {
  const chunks = lodash.chunk(items, batchSize);
  for (const chunk of chunks) {
    const items = await model.bulkCreate(chunk, params);
    inputStream.push(JSON.stringify(items));
  }
  inputStream.push(null); // Means end of stream
}


/**
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {string} method - action method
 * @param {number} batchSize - size of batches to fetch from database
 * @param {object} item - new parameters
 * @param {object} params - other Sequelize parameters
 */
async function performUpdateOrDestroy(model, inputStream, method, { batchSize = DEFAULT_BATCH_SIZE, ...params }, item) {
  const max = await model.count(params);
  const offset = 0;
  const offsets = [];
  let start = offset;
  while (start < max) {
    offsets.push(start);
    start += batchSize;
  }

  const schema = await model.describe();
  const primaryKey = Object.keys(schema).find(field => schema[field].primaryKey);

  for (const offset of offsets) {
    const difference = (batchSize + offset - max);
    const items = await model.findAll({
      ...params,
      offset: (method === 'update') ? offset : 0,
      limit: difference > 0 ? batchSize - difference : batchSize
    });

    const updatedItems = (method === 'update') ?
      await model.update(item, {
        ...params,
        where: {
          ...params.where,
          [primaryKey]: items.map(item => item[primaryKey])
        },
        offset,
        limit: difference > 0 ? batchSize - difference : batchSize,
      }) :
      await model.destroy({
        ...params,
        where: {
          ...params.where,
          [primaryKey]: items.map(item => item[primaryKey])
        }
      });
    inputStream.push(JSON.stringify(updatedItems));
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
 * @param {array} items - array of objects to create
 * @param {object} params - sequelize parameters
 * @return {object} - readable stream object
 */
function bulkCreateWithStream(items, params) {
  const model = this;
  const inputStream = new Readable({
    read: function() {},
  });
  performBulkCreate(model, inputStream, items, { batchSize: model.BATCH_SIZE, ...params });
  return inputStream;
}


/**
 * @param {object} item - new parameters
 * @param {object} params - sequelize parameters
 * @return {object} - readable stream object
 */
function updateWithStream(item, params) {
  const model = this;
  const inputStream = new Readable({
    read: function() {
    },
  });
  performUpdateOrDestroy(model, inputStream, 'update', { batchSize: model.BATCH_SIZE, ...params }, item);
  return inputStream;
}


/**
 * @param {object} params - sequelize parameters
 * @return {object} - readable stream object
 */
function destroyWithStream(params) {
  const model = this;
  const inputStream = new Readable({
    read: function() {
    },
  });
  performUpdateOrDestroy(model, inputStream, 'destroy', { batchSize: model.BATCH_SIZE, ...params });
  return inputStream;
}


/**
 * @param {object} sequelize - Sequelize object
 * @param {number} defaultBatchSize - default batch size
 */
function init(sequelize, defaultBatchSize) {
  for (const modelName of Object.keys(sequelize.models)) {
    sequelize.models[modelName].findAllWithStream = findAllWithStream;
    sequelize.models[modelName].bulkCreateWithStream = bulkCreateWithStream;
    sequelize.models[modelName].updateWithStream = updateWithStream;
    sequelize.models[modelName].destroyWithStream = destroyWithStream;
    if (!sequelize.models[modelName].BATCH_SIZE || defaultBatchSize) {
      sequelize.models[modelName].BATCH_SIZE = defaultBatchSize || DEFAULT_BATCH_SIZE;
    }
  }
}


module.exports = init;
