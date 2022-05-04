## dfstore 

This is an experimental package to implement a database agnostic API that supports various backend databases such as redis, mongodb, PostgreSQL and other SQL databases using the abstraction based on dataframe grids often used in data science projects that depend on python pandas or scala RDD or similar.  Use of dataframe in Go language implementation of dfstore depends on gota at 	

https://github.com/go-gota/gota

https://pkg.go.dev/github.com/go-gota/gota/dataframe#pkg-examples


## Example usage

### dfstore_test.go

1. Contains test code for postgresql/default usage
2. test code for redis/memory use case
3. test code for document/mongodb use case

### dataframe 

```
dataRows = [][]string{
		{"title", "artist", "price"},
		{"Blue Train", "John Coltrane", "56.99"},
		{"Giant Steps", "John Coltrane", "63.99"},
		{"Jeru", "Gerry Mulligan", "17.99"},
		{"Sarah Vaughan", "Sarah Vaughan", "34.98"},
	}
```

### Open databases

```
	dfs, err := dfstore.New(context.TODO(), dbtype)
	if err != nil {
		t.Errorf("cannot get new dfstore, %v",err)
		return
	}
	defer dfs.Close()
```

### write databases

```
	err = dfs.WriteRecords(dataRows)
	if err != nil {
		t.Errorf("cannot write, %v", err)
	}
```

### read databases

```
	filters := []dataframe.F{
		dataframe.F{Colname: "artist", Comparator: series.Eq, Comparando: "John Coltrane"},
		dataframe.F{Colname: "price", Comparator: series.Greater, Comparando: "60"},
	}
	res, err := dfs.ReadRecords(filters, 20)
```

## Testing

### Running a test PostgreSQL server

run postgresql server in a docker

```
docker run --rm --name postgresql -e POSTGRES_PASSWORD=password -p 5432:5432  -e POSTGRES_USER=pguser -e POSTGRES_DB=testdb postgres
```

run postgres cli in a docker
```
docker exec -it postgresql psql -h localhost -p 5432 -U pguser -W -d postgres
```

run the following in psql cli to create schema table
```
# CREATE DATABASE dfstore1;
# \c dfstore1;
# CREATE TABLE IF NOT EXISTS schema ( tablename VARCHAR(128) PRIMARY KEY, columns VARCHAR(255) NOT NULL );
# \l
```

### running a test redis server

```
docker run --name redis --rm -p 6379:6379 -d redis
```

Run redis-cli to require password login
```
$ redis-cli -a password
# config set requirepass password
```


### Running a test mongodb server

```
docker run -d --rm --name mongo -it -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=rootpass  mongo
```

#### Install mongosh

https://www.mongodb.com/docs/mongodb-shell/install/

#### Run mongosh

```
$ mongosh mongodb://root:rootpass@localhost:27017

test> db.dropDatabase('test')


test> use dfstore1
switched to db dfstore1

dfstore1> db.createCollection('table1')

dfstore1> db.getCollectionNames()

dfstore1>  db.createUser( { user: 'root',pwd: 'rootpass', roles: [ { role: "readWrite", db: "dfstore1"} ] } )

dfstore1> db.getUsers()

dfstore1> db.getCollection('table1').find().forEach(printjson)
{
  _id: ObjectId("000000000000000000000000"),
  artist: 'John Coltrane',
  price: 58.99,
  title: 'Blue Train'
}
{ _id: 1, title: 'Blue Train', artist: 'John Coltrane', price: 56.99 }
{ _id: 2, title: 'Giant Steps', artist: 'John Coltrane', price: 63.99 }
{ _id: 3, title: 'Jeru', artist: 'Gerry Mulligan', price: 17.99 }
{
  _id: 4,
  title: 'Sarah Vaughan',
  artist: 'Sarah Vaughan',
  price: 34.98
}

dfstore1>
```


### go test
```
go test
```
### go test a specific one
```
go test -run Memory
go test -run Doc 
go test -run Default
```