const fs = require('fs');
const csv = require('csv');
const path = require('path');
const http = require('http');
const cassandra = require('cassandra-driver');

const dataDir = process.env.HOME + '/Downloads/ml-latest-small';
const cassandraNode = 'default-machine';
const keyspace = 'test';
const client = new cassandra.Client({ contactPoints: [cassandraNode], keyspace: keyspace});

/*
   OMDB response format
   Format
   { Title: 'Love Potion No. 9',
Year: '1992',
Rated: 'PG-13',
Released: '13 Nov 1992',
Runtime: '92 min',
Genre: 'Comedy, Fantasy, Romance',
Director: 'Dale Launer',
Writer: 'Dale Launer',
Actors: 'Tate Donovan, Sandra Bullock, Mary Mara, Dale Midkiff',
Plot: 'Two scientists who are hopeless with the opposite sex invent a substance that makes them irresistible to anyone they speak to.',
Language: 'English',
Country: 'USA',
Awards: 'N/A',
Poster: 'http://ia.media-imdb.com/images/M/MV5BMTkwOTc1NzAyMV5BMl5BanBnXkFtZTcwMDA3NjUxMQ@@._V1_SX300.jpg',
Metascore: 'N/A',
imdbRating: '5.6',
imdbVotes: '10,136',
imdbID: 'tt0102343',
Type: 'movie',
Response: 'True' }
*/

/**
 * Execute a list of queries and return an array of promises
 * queries = [
 * {
 *  query:
 *  params:
 *  }
 *  ...
 *  ]
 */
function executeQueries(queries){
    return queries.map(function(q){
        return new Promise(function(resolve, reject) {
            client.execute(q.query, q.params, function(err, result) {
                if (err) {
                    console.log(err);
                    return reject(err);
                }
                console.log('completed: ' + q.query);
                resolve();
            });
        });
    });
}

/**
 * Loads table.csv and writes it to keyspace.table
 *
 * returns an array of promises
 */
function loadCassandraWithCSV(tables){
    return tables.map(function(table) {
        return new Promise(function(resolve, reject) {
            fs.readFile(dataDir + '/' + table + '.csv', function read(err, data) {
                if (err) {
                    console.log(err);
                    return reject(err);
                }
                csv.parse(data, {auto_parse: true}, function(err, output) {
                    if (err) {
                        console.log(err);
                        return reject(err);
                    }
                    var csvdata = output.splice(1); //remove header
                });
            });
        });
    });
}

var main = function(){
    const args = process.argv.slice(2);

    const tables = ['ratingsb', 'tagsb', 'moviesb', 'linksb', 'imdbb'];
    const schemas = [
        '(user int, movie int, rating double, PRIMARY KEY(user, movie))', //rating
        '(user int, movie int, tag varchar, PRIMARY KEY(user, movie))', //tag
        '(movie int PRIMARY KEY, name varchar, genres varchar)', //movie
        '(movie int PRIMARY KEY, imdb varchar, tmdb varchar)', //link
        ['(movie int PRIMARY KEY,', //imdb
            'imdb varchar,',
            'title varchar,',
            'year int,',
            'rated varchar,',
            'released varchar,',
            'runtime varchar,',
            'genre varchar,',
            'director varchar,',
            'writer varchar,',
            'actors varchar,',
            'plot varchar,',
            'language varchar,',
            'country varchar,',
            'awards varchar,',
            'poster varchar,',
            'metascore double,',
            'imdbrating double,',
            'imdbvotes int,',
            'imdbid varchar,',
            'type varchar)'].join(' ')
    ];

    var dropQueries = [];
    var createQueries = [];
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        var schema = schemas[i];
        if (args[0] == 'drop') {
            dropQueries.push({
                query: 'DROP TABLE IF EXISTS ' + table
            });
        }
        createQueries.push({
            query: ['CREATE TABLE IF NOT EXISTS',table,schema].join(' ')
        });
    }


    Promise.all(executeQueries(dropQueries))
    .then(function() {
        console.log('all dropped');
        return Promise.all(executeQueries(createQueries));
    })
    .then(function() {
        console.log('all created');
        return Promise.all(loadCassandraWithCSV(['movies']));
    })
    .then(function() {
        console.log('all loaded');
        process.exit(0);
    })
    .catch(console.error);

    /*
       const insertQuery = 'INSERT INTO imdb (movie, imdb, title, year, rated, '
       + 'released, runtime, genre, director, writer, actors, plot, language, '
       + 'country, awards, poster, metascore, imdbrating, imdbvotes, imdbid, '
       + 'type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';

       const query = 'SELECT movie, imdb FROM links';
       const backupDir = __dirname + '/backup'

       var count = 0;
       var callbacks = 0;

    //Limit http connections to avoid connection errors
    http.globalAgent.maxSockets = 50;

    //create table
    client.execute(creatQuery,
    function (err, res) {
    if(err) {
    console.info(err);
    }

    client.eachRow(query, [],
    function(n, row) {
    count += 1;
    callbacks += 2;

    http.get({
host: 'www.omdbapi.com',
path: '/?i=tt' + row.imdb
},
function(response) {
var body = '';
response.on('data', function(d) {
body += d;
});
response.on('end', function() {
const parsed = JSON.parse(body);

var metascore = parseFloat(parsed.Metascore);
var imdbrating = parseFloat(parsed.imdbRating);
var imdbvotes = parseInt(parsed.imdbVotes.replace(',',''))

if (isNaN(metascore)) metascore = -1;
if (isNaN(imdbrating)) imdbrating = -1;
if (isNaN(imdbvotes)) imdbvotes = -1;

client.execute(insertQuery,
    [parseInt(row.movie), row.imdb, parsed.Title, parseInt(parsed.Year), parsed.Rated,
        parsed.Released, parsed.Runtime, parsed.Genre, parsed.Director, parsed.Writer,
        parsed.Actors, parsed.Plot, parsed.Language, parsed.Country, parsed.Awards,
        parsed.Poster, metascore, imdbrating,
        imdbvotes, parsed.imdbID, parsed.Type],
        {prepare: true},
        function (err, res) {
            if(err) {
                console.info('movie tt' + row.imdb + ' failed to insert into cassandra');
                console.info(err);
            }
            callbacks -= 1;
            if (callbacks == 0) {
                console.log('callbacks completed')
                    process.exit();
            }
        });

        fs.mkdir(backupDir, function () {
                const file = path.resolve(backupDir + '/tt' + row.imdb);
                fs.unlink(file, function () {
                        fs.writeFile(file, JSON.stringify(parsed, null, 2), function(err) {
                                if(err) {
                                console.info('movie tt' + row.imdb + ' failed to write to file');
                                console.info(err);
                                }
                                callbacks -= 1;
                                if (callbacks == 0) {
                                console.log('callbacks completed')
                                process.exit();
                                }
                                });
                        });
                });
});
});
},
    function (err, result) {
        if(err) {
            console.info(err);
        }
        if (typeof result.nextPage == "function") {
            result.nextPage();
        }
        else {
            console.log('processed ' + count + ' rows')
        }
    }
);
}
);
*/


};

if (require.main === module) {
    main();
}
