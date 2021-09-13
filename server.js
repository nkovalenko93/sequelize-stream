const { Sequelize, Model, DataTypes } = require('sequelize');
const sequelizeStream = require('./index');
const { Writable } = require('stream');

const express = require('express')
const app = express()
const port = 3000

const sequelize = new Sequelize('tceihvon', 'tceihvon', 'Z3VGhEPZV9gaGgC1PneSptb8cK-LASeO', { host: 'kashin.db.elephantsql.com', port: 5432, dialect: 'postgres' });

class User extends Model {
}

User.init({ username: DataTypes.STRING }, { sequelize, modelName: 'user' });
sequelizeStream(sequelize, 20);

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

app.get('/getCsv', (req, res) => {
  res.set({
    'Content-Type': 'text/csv; charset=utf-8',
    'Content-Disposition': 'attachment; filename="testFile.csv"',
  })

  const stream = User.getCsvfindAllWithStream();
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
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})
