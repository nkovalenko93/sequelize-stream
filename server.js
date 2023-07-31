const { Sequelize, Model, DataTypes } = require('sequelize');
const sequelizeStream = require('./index');
const { Writable } = require('stream');

const express = require('express');
const app = express();
const port = 3000;

const sequelize = new Sequelize('stream', 'postgres', 'secretpass', {
  host: 'localhost',
  port: 5432,
  dialect: 'postgres'
});

class User extends Model {
}

User.init({
  id: { type: DataTypes.BIGINT.UNSIGNED, primaryKey: true, autoIncrement: true },
  bigintField: DataTypes.BIGINT,
  integerField: DataTypes.INTEGER,
  stringField: DataTypes.STRING,
  username: DataTypes.STRING
}, {
  sequelize,
  modelName: 'user',
});
sequelizeStream(sequelize, 2, false, true);

(async () => {
  await sequelize.sync();
  // for (let i = 0; i < 1000; i++) {
  //   await User.create({
  //     // id: `${(new Date()).getTime()}${i}`,
  //     // id: uuidv4(),
  //     bigintField: 5,
  //     integerField: 5,
  //     // numberField: 5,
  //     stringField: '5',
  //     username: `${(new Date()).getTime()}`
  //   });
  // }
})();

app.get('/test', (req, res) => {
  const stream = User.findAllWithStream();

  stream.on('data', chunk => {
    console.log('\n\nchunk', typeof chunk, chunk.toString());
  });
  stream.on('error', error => {
    console.log('\n\nERROR!!!!!', error);
    res.end(error);
  });
  stream.on('end', () => {
    console.log('\n\nEND!!!!!');
    res.end();
  });
  stream.pipe(res);
  setTimeout(() => {
    stream.destroy(new Error('some error'));
  }, 5000);
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
