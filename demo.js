var BCS = require('./bcs.js').BCS;
var fs = require('fs');

/*
var hostname = 'bcs-sandbox.baidu.com';
var ak = "EygRcI86pmJksQP9"; // 用户名
var sk = "kgS7LjDm9h0lVLpKs7m25igsHO9WNy8tH4"; // 密码
*/

var hostname = 'bcs.duapp.com';
var ak = 'F0f198f4eb66f1bdfdb9c7b268c38d99';
var sk = '26107f27ba8ff082b5f8e2a16f8e8fb7';


var options = {
    'host': hostname,
    'ak': ak,
    'sk': sk
}

var client = new BCS(options);
// client.get_object_info('develop', '/util.js', null, function(err, res){
// 	if (err) {
// 		console.log(res);
// 	} else {
// 		console.log(res);
// 	}

// });

client.is_object_exist('develop', '/util1.js', null, function(err, res){
	if (err) {
		console.log(res);
	} else {
		console.log( res ? "Object exist!" : "Object may not exist!");
	}
});
// client.create_object_by_content('develop', '/content.js', '我是中国人 你懂不？') 
// client.list_object('develop', null, 0);
