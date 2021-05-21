const { Sequelize, Model, DataTypes } = require('sequelize');
const sequelizeStream = require('./index');
const { Writable } = require('stream');

const express = require('express')
const app = express()
const port = 3000

const sequelize = new Sequelize('stream', 'stream', 'stream', { host: 'localhost', port: 5432, dialect: 'postgres' });

class User extends Model {
}

User.init({ username: DataTypes.STRING }, { sequelize, modelName: 'user' });
sequelizeStream(sequelize, 2);

(async () => {
  await sequelize.sync();
  // for (let i = 0; i < 1000; i++) {
  //   await User.create({ username: `${(new Date()).getTime()}` });
  // }
})();

app.get('/test', (req, res) => {
  const stream = User.findAllWithStream();

  stream.on('data', chunk => {
    console.log('\n\nchunk', typeof chunk, chunk.toString());
  });
  stream.on('error', error => {
    console.log('\n\nERROR!!!!!', error);
    res.end(error)
  });
  stream.on('end', () => {
    console.log('\n\nEND!!!!!');
    res.end()
  });
  stream.pipe(res);
  setTimeout(() => {
    stream.destroy(new Error('some error'));
  }, 5000);
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})
