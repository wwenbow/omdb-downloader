const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['default-machine'], keyspace: 'test'});

const query = 'SELECT email, last_name FROM user_profiles WHERE key=?';
client.execute(query, ['guy'], function(err, result) {
  assert.ifError(err);
  console.log('got user profile with email ' + result.rows[0].email);
});
