const fs = require('fs');
const path = require('path');
const http = require('http');
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['default-machine'], keyspace: 'test'});

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
const creatQuery = 'CREATE TABLE IF NOT EXISTS imdb ('
    + 'movie int PRIMARY KEY,'
    + 'imdb varchar,'
    + 'title varchar,'
    + 'year int,'
    + 'rated varchar,'
    + 'released varchar,'
    + 'runtime varchar,'
    + 'genre varchar,'
    + 'director varchar,'
    + 'writer varchar,'
    + 'actors varchar,'
    + 'plot varchar,'
    + 'language varchar,'
    + 'country varchar,'
    + 'awards varchar,'
    + 'poster varchar,'
    + 'metascore varchar,'
    + 'imdbrating double,'
    + 'imdbvotes int,'
    + 'imdbid varchar,'
    + 'type varchar);';

const insertQuery = 'INSERT INTO imdb (movie, imdb, title, year, rated, '
    + 'released, runtime, genre, director, writer, actors, plot, language, '
    + 'country, awards, poster, metascore, imdbrating, imdbvotes, imdbid, '
    + 'type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';

const query = 'SELECT movie, imdb FROM links LIMIT 400';
const backupDir = __dirname + '/backup'

var count = 0;
var callbacks = 0;

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

                            client.execute(insertQuery,
                                [parseInt(row.movie), row.imdb, parsed.Title, parseInt(parsed.Year), parsed.Rated,
                                    parsed.Released, parsed.Runtime, parsed.Genre, parsed.Director, parsed.Writer,
                                    parsed.Actors, parsed.Plot, parsed.Language, parsed.Country, parsed.Awards,
                                    parsed.Poster, parsed.Metascore, parseFloat(parsed.imdbRating),
                                    parseInt(parsed.imdbVotes.replace(',','')), parsed.imdbID, parsed.Type],
                                {prepare: true},
                                function (err, res) {
                                    if(err) {
                                        console.info('row ' + n + ' failed to insert into cassandra');
                                        console.info(err);
                                    }
                                    callbacks -= 1;
                                    if (callbacks == 0) {
                                        console.log('callbacks completed')
                                        process.exit();
                                    }
                                });

                            fs.mkdir(backupDir, function () {
                                var file = path.resolve(backupDir + '/tt' + row.imdb);
                                fs.unlink(file, function () {
                                    fs.writeFile(file, JSON.stringify(parsed, null, 2), function(err) {
                                        if(err) {
                                            console.info('row ' + n + ' failed to write to file');
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

