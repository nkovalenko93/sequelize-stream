const { Sequelize, Model, DataTypes } = require('sequelize');
const sequelizeStream = require('./index');


const sequelize = new Sequelize('stream', 'stream', 'stream', { host: 'localhost', port: 5432, dialect: 'postgres' });

class User extends Model {
}

User.init({ username: DataTypes.STRING }, { sequelize, modelName: 'user' });
sequelizeStream(sequelize, 2, true);

(async () => {
  await sequelize.sync();
  // for (let i = 0; i < 1000; i++) {
  //   await User.create({ username: `${(new Date()).getTime()}` });
  // }
  const stream = User.findAllWithStream();
  stream.on('data', chunk => {
    console.log('\n\nchunk', typeof chunk, chunk);
  });
  stream.on('error', error => {
    console.log('\n\nERROR!!!!!', error);
  });
  stream.on('end', () => {
    console.log('\n\nEND!!!!!');
  });
})();
