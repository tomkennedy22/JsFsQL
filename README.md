# JsFSQL

This is a light-weight package that allows for in-memory querying of semi-stuctured data, as well as write & reading the data to file. 
The package's big advantage is the heavily partitioned data structure, which provides the following benefits:
1. When querying based on primary keys, results return in O(1)
2. When querying based on partition keys, results return in ~O(n/p), n being total data size, p being number of partitions
3. When querying based on other keys, including partition keys in your query will greatly improve performance



--------------
**Create a database** (new or existing)
```
let db = new database({ dbname: "test_db", folder_path });
```

**Read from file. **If you already have data in file with the same db_name and folder_path specified above, you can read the DB file file -> memory
```
await db.read_from_file();
```

**Add a new table.** Choose indices based on frequently-used query patterns, as including them in queries will greatly improve performance
```
db.add_table({ table_name: "person_test", indices: ["birth_year", "birth_state"], primary_key: "person_id" });
```


**Insert data.** Data can either be single element or array of elements. Elements must have field with PK defined
```
db.tables.person_test.insert(data);
```


**Query data**. 
```
let results_1994_CA = db.tables.person.find({birth_year: 1994, birth_state: "CA"});
let results_1994 = db.tables.person.find({birth_year: 1994});
let results_pid_1 = db.tables.person.findOne({person_id: 1});
let results_pid_1_eq = db.tables.person.findOne({ person_id: { $eq: 1 } });
let results_matts = db.tables.person.find({name: "Matt"});
let results_1990_1994 = db.tables.person.find({$or: [{birth_year: 1990}, {birth_year: 1994}]});
let results_gt_1992 = db.tables.person.find({birth_year: {$gt: 1992}});
```

**Save database & write to file**
```
await db.save_database();
```


**Query operators**, largely using industry-standard
| Operator | Notes                                      |
|----------|--------------------------------------------|
| $eq      | Value equals                               |
| $ne      | Value not equal                            |
| $gt      | Value greater than                         |
| $gte     | Value greater than or equal to             |
| $lt      | Value less than                            |
| $lte     | Value less than or equal to                |
| $in      | Value in an array                          |
| $nin     | Value not in an array                      |
| $or      | Logical OR of multiple queries             |



---------------
### Development & use
When in the folder, run the below command to install all packages needed (really only fs & path)
```
npm install
```


To run the test script, simply run the below command
```
npx ts-node src/test.ts
```
