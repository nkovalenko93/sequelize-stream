# sequelize-stream

sequelize-stream is a library allowing to stream data with sequelize.

To have "findAllWithStream" in your models:

```
const sequelizeStream = require('node-sequelize-stream');
const sequelize = new Sequelize(...);
sequelizeStream(sequelize, defaultBatchSize);

```

defaultBatchSize - is an optional parameter that means amount of batches to be fetched with each chunk. Default value is 100.

```
const stream = db.models.User.findAllWithStream({batchSize: 50});
stream.pipe(res);
```

batchSize - is a default batch size for target fetching.

function findAllWithStream returns a readable stream.


Also batch size can be set for each user separately like this:
```
const User = instance.define('User', {...});
User.BATCH_SIZE = 10;
```

