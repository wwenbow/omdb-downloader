const fs = require('fs');
const csv = require('csv');
const path = require('path');
const http = require('http');
const cassandra = require('cassandra-driver');

const dataDir = process.env.HOME + '/Downloads/ml-20m';
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
 * Format data to the correct types
 */
var dataFormater = {
    movies : function(text){
        // movieId, title, genres
        return [ parseInt(text[0]), text[1], text[2] ];
    },
    ratings: function(text){
        // userId, movieId, rating
        return [ parseInt(text[0]), parseInt(text[1]), parseFloat(text[2]) ];
    },
    tags : function(text){
        // userId, movieId, tag
        return [ parseInt(text[0]), parseInt(text[1]), text[2] ];
    },
    links : function(text){
        // movieId, imdb, tmdb
        return [ parseInt(text[0]), text[1], text[2] ];
    },
    imdb : function(text){
        var ret = text;
        ret[0] = parseInt[ret[0]];
        ret[2] = parseInt[ret[2]];
        ret[16] = parseFloat[ret[16]];
        ret[17] = parseFloat[ret[17]];
        ret[18] = parseInt[ret[18]];
        return ret;
    }
};

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
function executeQueries(queries, options, log){
    return queries.map(function(q){
        return new Promise(function(resolve, reject) {
            client.execute(q.query, q.params, options, function(err, result) {
                if (err) {
                    console.error(err);
                    return reject(err);
                }
                if (log) {
                    console.log('completed: ' + q.query);
                }
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
function loadCassandraWithCSV(tables, schemas, delimiter, header, filetype){
    return tables.map(function(table) {
        return new Promise(function(resolve, reject) {
            fs.readFile(dataDir + '/' + table + filetype, function read(err, data) {
                if (err) {
                    console.log(err);
                    return reject(err);
                }
                csv.parse(data, {delimiter : delimiter}, function(err, output) {
                    if (err) {
                        console.log(err);
                        return reject(err);
                    }
                    var csvdata = [];
                    if (header) {
                        csvdata = output.splice(1); //remove header
                    } else {
                        csvdata = output;
                    }

                    var insertQueries = [];
                    var schema = schemas[table];
                    var ncols = schema.split(',').length;
                    var questionMarks = Array(ncols).fill('?').join(',');
                    questionMarks = '(' + questionMarks + ')';

                    for (var i = 0; i < csvdata.length; i++){
                        var insertQuery = {
                            query : 'INSERT INTO ' + table + ' ' + schema + ' VALUES' + questionMarks + ';',
                            params : dataFormater[table](csvdata[i])
                        };
                        insertQueries.push(insertQuery);
                    }

                    console.log('csv loaded ' + table);

                    Promise.all(executeQueries(insertQueries, { prepare: true },false))
                    .then(function(){
                        console.log('inserted ' + table);
                        resolve();
                    })
                    .catch(function(err){
                        console.error(err);
                        reject(err);
                    });
                });
            });
        });
    });
}


var main = function(){
    const args = process.argv.slice(2);

    const tables = ['ratings', 'tags', 'movies', 'links', 'imdb'];
    const schemas = {
        ratings : '(user int, movie int, rating double, PRIMARY KEY(user, movie))', //rating
        tags : '(user int, movie int, tag varchar, PRIMARY KEY(user, movie))', //tag
        movies : '(movie int PRIMARY KEY, name varchar, genres varchar)', //movie
        links : '(movie int PRIMARY KEY, imdb varchar, tmdb varchar)', //link
        imdb : ['(movie int PRIMARY KEY,', //imdb
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
    };

    const colNames = {
        ratings : '(user, movie, rating)', //rating
        tags : '(user, movie, tag)', //tag
        movies : '(movie, name, genres)', //movie
        links : '(movie, imdb, tmdb)', //link
        imdb : ['(movie,', //imdb
            'imdb,',
            'title,',
            'year,',
            'rated,',
            'released,',
            'runtime,',
            'genre,',
            'director,',
            'writer,',
            'actors,',
            'plot,',
            'language,',
            'country,',
            'awards,',
            'poster,',
            'metascore,',
            'imdbrating,',
            'imdbvotes,',
            'imdbid,',
            'type)'].join(' ')
    };

    var dropQueries = [];
    var createQueries = [];
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        var schema = schemas[table];
        if (args[0] == 'drop') {
            dropQueries.push({
                query: 'DROP TABLE IF EXISTS ' + table
            });
        }
        createQueries.push({
            query: ['CREATE TABLE IF NOT EXISTS',table,schema].join(' ')
        });
    }


    Promise.all(executeQueries(dropQueries, {}, true))
    .then(function() {
        console.log('all dropped');
        return Promise.all(executeQueries(createQueries, {}, true));
    })
    .then(function() {
        console.log('all created');
        return Promise.all(loadCassandraWithCSV(tables.splice(0, 4),
                                                colNames, ',', true, '.csv'));
    })
    .then(function() {
        console.log('all created');
        return Promise.all(loadCassandraWithCSV([ 'imdb' ] ,
                                                colNames, '|', false, '.psv'));
    })
    .then(function() {
        console.log('all loaded');
        process.exit(0);
    })
    .catch(function(err) {
        console.error(err);
        process.exit(1);
    });
};

if (require.main === module) {
    main();
}
