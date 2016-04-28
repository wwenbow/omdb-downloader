const fs = require('fs');
const csv = require('csv');
const path = require('path');
const http = require('http');

const dataDir = process.env.HOME + '/Downloads/ml-20m';
const psvFile = 'imdb.psv';

/*
 * Save omdb data as pipe separated file
 *
 *
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

//Limit http connections to avoid connection errors
http.globalAgent.maxSockets = 50;

var psvData = [];

fs.readFile(dataDir + '/links.csv', function read(err, data) {
    if (err) {
        console.err(err.stack);
        return reject(err);
    }
    csv.parse(data, function(err, output) {
        if (err) {
            console.err(err.stack);
            return reject(err);
        }
        const links = output.splice(1); //remove header
        console.log(links[2]);

        Promise.all(links.map(function(link) {
            return new Promise(function(resolve, reject) {
                const movie = link[0];
                const imdb = link[1];

                http.get({
                    host: 'www.omdbapi.com',
                    path: '/?i=tt' + imdb
                },
                function(response) {
                    var body = '';
                    response.on('error', function(err) {
                        // This prints the error message and stack trace to `stderr`.
                        console.error(err.stack);
                        return reject(err);
                    });
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

                        psvData.push([parseInt(movie), imdb, parsed.Title, parseInt(parsed.Year), parsed.Rated,
                                     parsed.Released, parsed.Runtime, parsed.Genre, parsed.Director, parsed.Writer,
                                     parsed.Actors, parsed.Plot, parsed.Language, parsed.Country, parsed.Awards,
                                     parsed.Poster, metascore, imdbrating,
                                     imdbvotes, parsed.imdbID, parsed.Type].join('|'));
                        console.log('downloaded ' + imdb);
                        resolve();
                    }); //response end
                }); //get
            }); //new promise
        }))
        .then(function() {
            console.log('data downloaded');
            fs.unlink(psvFile, function () {
                fs.writeFile(psvFile, psvData.join('\n'), function(err) {
                    if(err) {
                        console.err(err.stack);
                        process.exit(1);
                    }
                    console.log('psv saved');
                    process.exit(0);
                });
            });
        })
        .catch(function(err) {
            console.error(err.stack);
            process.exit(1);
        });
    });
});



