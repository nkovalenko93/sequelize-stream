'use strict';

const { Readable } = require('stream');
const { Op } = require('sequelize');
const splitByChunks = require('lodash.chunk');

const DEFAULT_BATCH_SIZE = 100;


const getIsObjectMode = (model, params) => {
  let objectMode = params.isObjectMode;
  if ((typeof objectMode) !== 'boolean') {
    objectMode = model.IS_OBJECT_MODE || false;
  }
  return (objectMode || false);
};


const getIsSingleArrayMode = (model, params) => {
  let signleArrayMode = params.isSingleArrayMode;
  if ((typeof signleArrayMode) !== 'boolean') {
    signleArrayMode = model.IS_SINGLE_ARRAY_MODE || false;
  }
  return (signleArrayMode || false);
};


const createReadableStream = (model, params = {}) => {
  return new Readable({
    objectMode: getIsObjectMode(model, params),
    read: function() {
    },
  });
};


/**
 * @param {string} primaryKeyField - Sequelize model primary key
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {number} batchSize - size of batches to fetch from database
 * @param {number} limit - Sequelize limit parameter
 * @param {number} offset - Sequelize offset parameter
 * @param {object} params - other Sequelize parameters
 */
async function performSearchByPrimaryKey(primaryKeyField, model, inputStream, { batchSize = DEFAULT_BATCH_SIZE, limit, offset = 0, ...params }) {
  const isObjectMode = getIsObjectMode(model, params);
  const isSingleArrayMode = getIsSingleArrayMode(model, params);
  if (!inputStream.destroyed && isSingleArrayMode && !isObjectMode) {
    inputStream.push('[');
  }
  if (!inputStream.destroyed) {
    let isFinished = false;
    let lastId = 0;
    while (!isFinished) {
      let where = { ...(params.where || {}) };
      if (where[primaryKeyField]) {
        where = Op.and(where, { [primaryKeyField]: { [Op.gt]: lastId } });
      } else {
        where[primaryKeyField] = { [Op.gt]: lastId };
      }
      const items = (
        await model.findAll({
          ...params,
          where,
          ...(lastId ? {} : { offset }),
          limit: batchSize,
        })
      ).map(item => item.toJSON());
      if (!inputStream.destroyed) {
        let dataToPush = isObjectMode ? items : JSON.stringify(items);
        if (isSingleArrayMode && !isObjectMode) {
          dataToPush = dataToPush.substring(1, dataToPush.length - 1);
          if (lastId && items.length) {
            dataToPush = `,${dataToPush}`;
          }
        }
        inputStream.push(dataToPush);
        if (items.length < batchSize) {
          isFinished = true;
        } else {
          lastId = items[items.length - 1].id;
        }
      }
    }
  }
  if (!inputStream.destroyed) {
    if (isSingleArrayMode && !isObjectMode) {
      inputStream.push(']');
    }
    inputStream.push(null); // Means end of stream
  }
}


/**
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {number} batchSize - size of batches to fetch from database
 * @param {number} limit - Sequelize limit parameter
 * @param {number} offset - Sequelize offset parameter
 * @param {object} params - other Sequelize parameters
 */
async function performSearch(model, inputStream, { batchSize = DEFAULT_BATCH_SIZE, limit, offset = 0, ...params }) {
  try {
    const primaryKeyField = model.primaryKeyAttributes[0];
    if (primaryKeyField) {
      const fieldDefinition = model.getAttributes()[primaryKeyField];
      const isAutoIncrement = (fieldDefinition.primaryKey && (fieldDefinition.autoIncrement || fieldDefinition.autoIncrementIdentity));
      if (isAutoIncrement) {
        return performSearchByPrimaryKey(
          primaryKeyField,
          model,
          inputStream,
          { batchSize, limit, offset, ...params },
        );
      }
    }
    let max = limit;
    if (!max) {
      max = await model.count({ ...params, attributes: undefined, limit, offset });
    }
    const offsets = [];
    let start = offset;
    while (start < max) {
      offsets.push(start);
      start += batchSize;
    }
    const isObjectMode = getIsObjectMode(model, params);
    const isSingleArrayMode = getIsSingleArrayMode(model, params);
    if (!inputStream.destroyed && isSingleArrayMode && !isObjectMode) {
      inputStream.push('[');
    }
    if (!inputStream.destroyed) {
      let isFirst = true;
      for (const offset of offsets) {
        if (!inputStream.destroyed) {
          const difference = (batchSize + offset - max);
          if (!inputStream.destroyed) {
            const items = (
              await model.findAll({
                ...params,
                offset,
                limit: difference > 0 ? batchSize - difference : batchSize,
              })
            ).map(item => item.toJSON());
            if (!inputStream.destroyed) {
              let dataToPush = isObjectMode ? items : JSON.stringify(items);
              if (isSingleArrayMode && !isObjectMode) {
                dataToPush = dataToPush.substring(1, dataToPush.length - 1);
                if (!isFirst && items.length) {
                  dataToPush = `,${dataToPush}`;
                }
              }
              if (items.length) {
                inputStream.push(dataToPush);
              }
              if (!items.length || (items.length < batchSize)) {
                break;
              }
              if (isFirst) {
                isFirst = false;
              }
            }
          }
        }
      }
    }
    if (!inputStream.destroyed) {
      if (isSingleArrayMode && !isObjectMode) {
        inputStream.push(']');
      }
      inputStream.push(null); // Means end of stream
    }
  } catch (err) {
    if (!inputStream.destroyed) {
      inputStream.destroy(err);
    }
  }
}


/**
 * @param {object} model - Sequelize model
 * @param {object} inputStream - readable stream object
 * @param {array} items - items to be created
 * @param {number} batchSize - size of batches to fetch from database
 */
async function performBulkCreate(model, inputStream, items, { batchSize = DEFAULT_BATCH_SIZE, ...params }) {
  try {
    const chunks = splitByChunks(items, batchSize);
    if (!inputStream.destroyed) {
      for (const chunk of chunks) {
        if (!inputStream.destroyed) {
          const items = await model.bulkCreate(chunk, params);
          if (!inputStream.destroyed) {
            inputStream.push(JSON.stringify(items));
          }
        }
      }
    }
    if (!inputStream.destroyed) {
      inputStream.push(null); // Means end of stream
    }
  } catch (err) {
    if (!inputStream.destroyed) {
      inputStream.destroy(err);
    }
  }
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
  try {
    const max = await model.count({ ...params, attributes: undefined });
    const offset = 0;
    const offsets = [];
    let start = offset;
    while (start < max) {
      offsets.push(start);
      start += batchSize;
    }

    const schema = await model.describe();
    const primaryKey = Object.keys(schema).find(field => schema[field].primaryKey);

    if (!inputStream.destroyed) {
      for (const offset of offsets) {
        if (!inputStream.destroyed) {
          const difference = (batchSize + offset - max);
          const items = await model.findAll({
            ...params,
            offset: (method === 'update') ? offset : 0,
            limit: difference > 0 ? batchSize - difference : batchSize
          });

          if (!inputStream.destroyed) {
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
            if (!inputStream.destroyed) {
              inputStream.push(JSON.stringify(updatedItems));
            }
          }
        }
      }
    }
    if (!inputStream.destroyed) {
      inputStream.push(null); // Means end of stream
    }
  } catch (err) {
    if (!inputStream.destroyed) {
      inputStream.destroy(err);
    }
  }
}


/**
 * @param {object} params - Sequelize parameters
 * @return {object} - readable stream object
 */
function findAllWithStream(params) {
  const model = this;
  const inputStream = createReadableStream(model, params);
  if (!inputStream.destroyed) {
    performSearch(model, inputStream, { batchSize: model.BATCH_SIZE, ...params });
  }
  return inputStream;
}


/**
 * @param {array} items - array of objects to create
 * @param {object} params - sequelize parameters
 * @return {object} - readable stream object
 */
function bulkCreateWithStream(items, params) {
  const model = this;
  const inputStream = createReadableStream(model, params);
  if (!inputStream.destroyed) {
    performBulkCreate(model, inputStream, items, { batchSize: model.BATCH_SIZE, ...params });
  }
  return inputStream;
}


/**
 * @param {object} item - new parameters
 * @param {object} params - sequelize parameters
 * @return {object} - readable stream object
 */
function updateWithStream(item, params) {
  const model = this;
  const inputStream = createReadableStream(model, params);
  if (!inputStream.destroyed) {
    performUpdateOrDestroy(model, inputStream, 'update', { batchSize: model.BATCH_SIZE, ...params }, item);
  }
  return inputStream;
}


/**
 * @param {object} params - sequelize parameters
 * @return {object} - readable stream object
 */
function destroyWithStream(params) {
  const model = this;
  const inputStream = createReadableStream(model, params);
  if (!inputStream.destroyed) {
    performUpdateOrDestroy(model, inputStream, 'destroy', { batchSize: model.BATCH_SIZE, ...params });
  }
  return inputStream;
}


/**
 * @param {object} sequelize - Sequelize object
 * @param {number} defaultBatchSize - default batch size
 */
function init(sequelize, defaultBatchSize, isObjectMode = false) {
  for (const modelName of Object.keys(sequelize.models)) {
    sequelize.models[modelName].findAllWithStream = findAllWithStream;
    sequelize.models[modelName].bulkCreateWithStream = bulkCreateWithStream;
    sequelize.models[modelName].updateWithStream = updateWithStream;
    sequelize.models[modelName].destroyWithStream = destroyWithStream;
    if (!sequelize.models[modelName].BATCH_SIZE || defaultBatchSize) {
      sequelize.models[modelName].BATCH_SIZE = defaultBatchSize || DEFAULT_BATCH_SIZE;
    }
    if ((typeof sequelize.models[modelName].IS_OBJECT_MODE) !== 'boolean') {
      sequelize.models[modelName].IS_OBJECT_MODE = isObjectMode;
    }
  }
}


module.exports = init;

