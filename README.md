# node-sequelize-stream

node-sequelize-stream is a library allowing to stream data with sequelize.

`npm install node-sequelize-stream`

To have "findAllWithStream" method in your models you need to pass your sequelize instance to `sequelizeStream` function:

```
const sequelizeStream = require('node-sequelize-stream');
const sequelize = new Sequelize(...);
sequelizeStream(sequelize, defaultBatchSize);

```

`defaultBatchSize` - is an optional parameter that means default amount of batches to be fetched with each chunk for all models. Default value is 100.

To get stream object you need to do:
```
const stream = db.models.User.findAllWithStream({batchSize: 50});
stream.pipe(res);
```

```
const stream = db.models.User.bulkCreateWithStream([{id: 1, name: 'SomeUser'}, ...], {batchSize: 50});
stream.pipe(res);
```

```
const stream = db.models.User.updateWithStream([{name: 'UpdatedName'}, ...], {batchSize: 50, where: {...}});
stream.pipe(res);
```

```
const stream = db.models.User.destroyWithStream({batchSize: 50, where: {...}});
stream.pipe(res);
```

`batchSize` - is an optional parameter that means default batch size for target action (model batch size or default batch size will be taken if parameter is not defined).


Also batch size can be set for each model separately like this:
```
const User = instance.define('User', {...});
User.BATCH_SIZE = 10;
```
