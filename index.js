var net         = require('net');
var Memcached   = require('memcached');
var mysql       = require('mysql');
var mysqlConfig = require('./mysql.json');
var pool        = mysql.createPool(mysqlConfig);

function feed(rows, conn){
    var count = Math.floor(rows.length / 1000);
    count     = (rows.length % 1000 === 0? count:count + 1);
    var cur   = 0;
    console.log(rows);
    for (var i = 0; i < count; i++) {
        var sql    = 'replace into post_feed(post_id, user_id, forward_id) values';
        var params = [];
        for (var j = 0; j < ((i + 1) * 1000 > rows.length? rows.length : (i + 1) * 1000); j++) {
            sql += ' (?, ?, ?),';
            params.push(data.post_id);
            params.push(rows[j].target);
            params.push(data.forward_id);
        };
        sql = sql.substring(0, sql.length - 1);
        conn.query(sql, params, function(insert_err, result){
            if (insert_err) {
                console.log(insert_err);
            };
            cur++;
            if (cur == count) {
                conn.release();
            };
        });
    };
}

var server    = net.createServer(function(conn){
    conn.on('data', function(data) {
        data = JSON.parse(data.toString());
        var memcached = new Memcached('127.0.0.1:11211');
        if (data.post_id == null) {
            return;
        };
        if (data.user_id == null) {
            return;
        };
        if (!data.hasOwnProperty('forward_id')) {
            data.forward_id = 0;
        };
        var userRelation = memcached.get('user.relation.' + data.user_id);
        if (userRelation == []) {
            return;
        };
        if (!userRelation) {
            pool.getConnection(function(err,conn){  
                if(err){  
                    console.log(err);
                }else{  
                    conn.query("select target from user_relation where status in (0,1) and origin = ?", [data.user_id], function(err,rows,fields){    
                        if (err) {
                            console.log(err);
                        };
                        memcached.set('user.relation.' + data.user_id, rows);
                        feed(rows, conn);
                    });
                }
            });  
        }
        else{
            pool.getConnection(function(err,conn){
                feed(userRelation, conn);
            });
        }

    });
});
server.on('error', function(err){
    console.log(err);
});
server.listen('/dev/shm/feed.sock');