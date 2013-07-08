/**
 * Baidu BCS Nodejs Version
 * Author Liu Chao
 */
var http = require('http');
var path = require('path');
var crypto = require('crypto');
var Url = require('url');
var querystring = require('querystring');
var fs = require('fs');
var mime = require('mime');

var DEBUG = true;

//ACL_TYPE:
//公开读权限
//公开写权限（不具备删除权限）
//公开读写权限（不具备删除权限）
//公开所有权限
//私有权限，仅bucket所有者具有所有权限
//SDK中开放此上五种acl_tpe
var ACL_TYPES = ["public-read", "public-write", "public-read-write", "public-control", "private"];

function isOK(status) {
    var codes = [200, 201, 204, 206];
    return codes.indexOf(status) !== -1;
}


var BCS = function(option) {
    this.host = option.host;
    this.ak = option.ak;
    this.sk = option.sk;
    //superfile 每个object分片后缀
    this.BCS_SUPERFILE_POSTFIX = option.postfix || '_bcs_superfile_';
    //sdk superfile分片大小 ，单位 B（字节）
    this.BCS_SUPERFILE_SLICE_SIZE = option.slicesize || 1024 * 1024;
    //是否使用ssl
    this.use_ssl = false;
};

/**
 * 将消息发往Baidu BCS.
 * @param array $opt
 * @return BCS_ResponseCore
 */
BCS.prototype.authenticate = function(opt) {
    var boundaryKey = Math.random().toString(16); // random string
    var response = {};
    // console.log(opt);
    // Validate the S3 bucket name, only list_bucket didnot need validate_bucket
    if (! ('/' == opt['object'] && '' == opt['bucket'] && 'GET' == opt['method'] 
        && !(opt['query_string'] && opt['query_string']['acl']) ) 
        && !this.validate_bucket(opt['bucket'])) {
        // throw new BCS_Exception ( $opt [self::BUCKET] . 'is not valid, please check!' );
        console.log(opt['bucket'] + 'is not valid, please check!' );
        return false;
    }
    //Validate object
    if ( opt['object']  && !this.validate_object(opt['object']) ) {
        // throw new BCS_Exception ( "Invalid object param[" . $opt [self::OBJECT] . "], please check.", - 1 );
        console.log( "Invalid object param[" + opt['object'] + "], please check.", - 1 );
    }
    //construct url
    var url = this.format_url( opt );
    if (url === false) {
        // throw new BCS_Exception ( 'Can format url, please check your param', - 1 );
        console.log( 'Can format url, please check your param', - 1 );
    }
    opt['url'] = url;
    console.log(url);
    //build request
    if (opt['method']) {
        var method = opt['method'];
    }

    var options = Url.parse(url);
    options['method'] = method;
    /*
    //Write get_object content to fileWriteTo
    if (isset ( $opt ['fileWriteTo'] )) {
        $request->set_write_file ( $opt ['fileWriteTo'] );
    }
    

    */
    var req = http.request(options, function(res) {
        response['status'] = res.statusCode;
        response['headers'] = JSON.stringify(res.headers);
        response['body'] = "";
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            console.log('BODY: ' + chunk);
            // response['body'] += chunk;
        });
        res.on('end', function () {
            opt['callback'](false, response);
            // console.log(response);
        });
    });
    // Merge the HTTP headers
    if (opt['headers']) {
        for (var header in opt['headers']) {
            req.setHeader(header, opt['headers']);
        }
    }
    req.on('error', function(e) {
      opt['callback'](true, e);
      console.log('problem with request: ' + e.message);
    });
    req.setHeader('Content-Type', 'application/x-www-form-urlencoded');


// ###################################################################################### / 
    if (opt['fileUpload']) {
        // Upload file

        if (!fs.existsSync( opt['fileUpload'] )) {
            throw( 'File[' + opt['fileUpload'] + '] not found!', - 1 );
        }
       
        var mimetype = mime.lookup(opt['fileUpload']);
        var file_size = fs.statSync(opt['fileUpload']).size,
            length = file_size;

        if (opt["length"]) {
            if (opt ["length"] > file_size) {
                throw( "Input opt[length] invalid! It can not bigger than file-size", - 1 );
            }
            length = opt['length'];
        }
        if (opt['seekTo'] && !opt["length"]) {
            // Read from seekTo until EOF by default, when set seekTo but not set $opt["length"]
            length -= opt ['seekTo'];
        }

        // Attempt to guess the correct mime-type
        if ( req.getHeader('Content-Type') === 'application/x-www-form-urlencoded' ) {
            req.setHeader('Content-Type', mimetype);
        }
        //          }
        req.setHeader('Content-Length', length);
        req.setHeader('Content-MD5', '');
        req.setHeader('Content-Type', 'multipart/form-data; boundary="' + boundaryKey + '"');
        // the header for the one and only part (need to use CRLF here)
        req.write( 
            '--' + boundaryKey + '\r\n'
            // use your file's mime type here, if known
            + 'Content-Type: application/octet-stream\r\n' 
            // "name" is the name of the form field
            // "filename" is the name of the original file
            + 'Content-Disposition: form-data; name="my_file"; filename="my_file.bin"\r\n'
            + 'Content-Transfer-Encoding: binary\r\n\r\n' 
        );
        fs.createReadStream(opt['fileUpload'], { bufferSize: 4 * 1024 })
            .on('end', function() {
                // mark the end of the one and only part
                req.end('\r\n--' + boundaryKey + '--'); 
            })
            // set "end" to false in the options so .end() isn't called on the request
            .pipe(req, { end: false }) // maybe write directly to the socket here?

// ###################################################################################### //

        // var filestream = fs.createReadStream(opt['fileUpload'], {'start' : opt['seekTo'], 'end' : (length - opt['seekTo']) });
        // filestream.pipe(req);
    } else {

        // Set content to Http-Body
        if (opt['content']) {
            var buf = new Buffer(opt['content']);
            req.setHeader('Content-Length', buf.length);
            req.write(opt['content']);
        }
        req.end();
    }    
};
BCS.prototype.log = function()  {
    
}
/**
 * 获取当前密钥对拥有者的bucket列表
 * @param array opt (Optional) 
 * BaiduBCS::IMPORT_BCS_LOG_METHOD - String - Optional: 支持用户传入日志处理函数，函数定义如 function f(log)
 * @throws BCS_Exception
 * @return BCS_ResponseCore
 */
BCS.prototype.list_bucket = function(opt) {
    var opt = opt || {}, response;
    //opt array
    /*
    if ($opt != NULL && ! is_array ( $opt )) {
        throw new BCS_Exception ( '$opt must be array, please check!', - 1 );
    }
    */
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = '';
    opt['method'] = 'GET';
    opt['object'] = '/';
    var response = this.authenticate(opt);
    // console.log( $response->isOK () ? "List bucket success!" : "List bucket failed! Response: [" . $response->body . "]", $opt );
    // return response;
};
/**
 * 创建 bucket
 * @param string $bucket (Required) bucket名称
 * @param string $acl (Optional)    bucket权限设置，若为null，使用server分配的默认权限 
 * @ACL_TYPES = ["public-read", "public-write", "public-read-write", "public-control", "private"];
 * @param array $opt (Optional) 
 * acl_type 可在创建bucket时通过acl_type指定bucket的acl权限设置，可在BaiduBCS::$ACL_TYPES选择查看权限可选项和配置
 * @throws BCS_Exception
 * @return BCS_ResponseCore
 */
BCS.prototype.create_bucket = function(bucket, acl, opt) {
    var acl = acl || null,
        opt = opt || {};
    //构造 opt object
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['method'] = 'PUT';
    opt['object'] = '/';
    if (null !== acl) {
        //valid acl_type
        if (ACL_TYPES.indexOf(acl) === -1) {
            // throw new BCS_Exception ( "Invalid acl_type[" . $opt ['acl_type'] . "], please check!", - 1 );
            console.log( "Invalid acl_type[" + acl + "], please check!", - 1 );
        }
        opt['acl_type'] = acl;
        this.set_header_into_opt("x-bs-acl", acl, opt);
    }
    //authenticate
    var response = this.authenticate(opt);
    // this.log( $response->isOK () ? "Create bucket success!" : "Create bucket failed! Response: [" . $response->body . "]", $opt );
    // return $response;
}
/**
 * 删除bucket
 * @param string $bucket (Required)
 * @param array $opt (Optional)
 * @return boolean|BCS_ResponseCore
 */
BCS.prototype.delete_bucket = function(bucket, opt) {
    // body...
    var opt = opt || {};
    //opt array
    /*
    if ($opt != NULL && ! is_array ( $opt )) {
        throw new BCS_Exception ( '$opt must be array, please check!', - 1 );
    }
    */
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['method'] = 'DELETE';
    opt['object'] = '/';
    var response = this.authenticate(opt);
    // $this->log ( $response->isOK () ? "Delete bucket success!" : "Delete bucket failed! Response: [" . $response->body . "]", $opt );
    // return $response;
};
/**
 * 获取bucket的acl
 * @param string $bucket (Required)
 * @param array $opt (Optional)
 * @return BCS_ResponseCore
 */
BCS.prototype.get_bucket_acl = function(bucket, opt) {
    var opt = opt || {};

    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['method'] = 'GET';
    opt['object'] = '/';
    opt['query_string'] = {acl : 1};
    var response = this.authenticate(opt);
    // $this->log ( $response->isOK () ? "Get bucket acl success!" : "Get bucket acl failed! Response: [" . $response->body . "]", $opt );
    // return $response;
};
/**
 * 获取bucket中object列表
 * @param string $bucket (Required)
 * @param array $opt (Optional)
 * start : 主要用于翻页功能，用法同mysql中start的用法
 * limit : 主要用于翻页功能，用法同mysql中limit的用法
 * prefix: 只返回以prefix为前缀的object，此处prefix必须以'/'开头
 * @throws BCS_Exception
 * @return BCS_ResponseCore
 */
BCS.prototype.list_object = function(bucket, opt) {
    var opt = opt || {};

    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    if (!bucket) {
        throw( "Bucket should not be empty, please check", - 1 );
    }
    opt['method'] = 'GET';
    opt['object'] = '/';
    opt['query_string'] = {};

    if (opt['start']) {
        opt['query_string']['start'] = opt['start'];
    }
    if (opt['limit']) {
        opt['query_string']['limit'] = opt ['limit'];
    }
    if (opt['prefix']) {
        opt['query_string']['prefix'] = encodeURIComponent(opt['prefix']);
    }
    var response = this.authenticate(opt);
    // $this->log ( $response->isOK () ? "List object success!" : "Lit object failed! Response: [" . $response->body . "]", $opt );
    // return $response;
};
/**
 * 以目录形式获取bucket中object列表
 * @param string $bucket (Required)
 * @param $dir (Required)
 * 目录名，格式为必须以'/'开头和结尾，默认为'/'
 * @param string $list_model (Required)
 * 目录展现形式，值可以为0,1,2，默认为2，以下对各个值的功能进行介绍：
 * 0->只返回object列表，不返回子目录列表
 * 1->只返回子目录列表，不返回object列表 
 * 2->同时返回子目录列表和object列表
 * @param array $opt (Optional)
 * start : 主要用于翻页功能，用法同mysql中start的用法
 * limit : 主要用于翻页功能，用法同mysql中limit的用法
 * @throws BCS_Exception
 * @return BCS_ResponseCore
 */
BCS.prototype.list_object_by_dir = function(bucket, dir, list_model, opt) {
    var opt = opt || {},
        dir = dir || '/',
        list_model = list_model || 2;
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    if (!bucket) {
        throw( "Bucket should not be empty, please check", - 1 );
    }
    opt['method'] = 'GET';
    opt['object'] = '/';
    opt['query_string'] = {};

    if (opt['start']) {
        opt['query_string']['start'] = opt['start'];
    }
    if (opt['limit']) {
        opt['query_string']['limit'] = opt ['limit'];
    }
    if (opt['prefix']) {
        opt['query_string']['prefix'] = encodeURIComponent(opt['dir']);
    }
    if (opt['dir']) {
        opt['query_string']['dir'] = list_model;
    }    
    var response = this.authenticate(opt);
    // $this->log ( $response->isOK () ? "List object success!" : "Lit object failed! Response: [" . $response->body . "]", $opt );
    // return $response;
};
/**
 * 上传文件
 * @param string $bucket (Required) 
 * @param string $object (Required) 
 * @param string $file (Required); 需要上传的文件的文件路径
 * @param array $opt (Optional) 
 * filename - Optional; 指定文件名
 * acl - Optional ; 上传文件的acl，只能使用acl_type
 * seekTo - Optional; 上传文件的偏移位置
 * length - Optional; 待上传长度
 * @return BCS_ResponseCore
 */
BCS.prototype.create_object = function(bucket, object, file, opt) {
    var opt = opt || {}; 
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['object'] = object;
    opt['fileUpload'] = file;
    opt['method'] = 'PUT';

    if (opt['acl']) {
        if (ACL_TYPES.indexOf(acl) === -1) {
            // throw new BCS_Exception ( "Invalid acl_type[" . $opt ['acl_type'] . "], please check!", - 1 );
            console.log( "Invalid acl_type[" + acl + "], please check!", - 1 );
        }
        this.set_header_into_opt("x-bs-acl", acl, opt);
        //TODO unset ( $opt ['acl'] );
    }
    if (opt['filename']) {
        this.set_header_into_opt( "Content-Disposition", 'attachment; filename=' + opt['filename'], opt );
    }
    /*
     else {
        //从object中提取filename，如object 为'/a/b/c.txt' ，提取的filename 为c.txt
        $arr_tmp = explode ( '/', $object );
        $filename_tmp = $arr_tmp [count ( $arr_tmp ) - 1];
        self::set_header_into_opt ( "Content-Disposition", 'attachment; filename=' . $filename_tmp, $opt );
    }
    */
    var response = this.authenticate(opt);
    // $this->log ( $response->isOK () ? "Create object[$object] file[$file] success!" : "Create object[$object] file[$file] failed! Response: [" . $response->body . "] Logid[" . $response->header ["x-bs-request-id"] . "]", $opt );
    // return $response;
};
/**
 * 上传文件
 * @param string $bucket (Required) 
 * @param string $object (Required) 
 * @param string $file (Required); 需要上传的文件的文件路径
 * @param array $opt (Optional) 
 * filename - Optional; 指定文件名
 * acl - Optional ; 上传文件的acl，只能使用acl_type
 * @return BCS_ResponseCore
 */
BCS.prototype.create_object_by_content = function(bucket, object, content, opt) {
    var opt = opt || {}; 
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['object'] = object;
    opt['method'] = 'PUT';

    if (content && typeof content === 'string') {
        opt['content'] = content;
    } else {
        throw( "Invalid object content, please check.", - 1 );
    }
    if (opt['acl']) {
        if (ACL_TYPES.indexOf(acl) === -1) {
            // throw new BCS_Exception ( "Invalid acl_type[" . $opt ['acl_type'] . "], please check!", - 1 );
            console.log( "Invalid acl_type[" + acl + "], please check!", - 1 );
        }
        this.set_header_into_opt("x-bs-acl", acl, opt);
        //TODO unset ( $opt ['acl'] );
    }
    if (opt['filename']) {
        this.set_header_into_opt( "Content-Disposition", 'attachment; filename=' + opt['filename'], opt );
    }
    var response = this.authenticate(opt);
    // $this->log ( $response->isOK () ? "Create object[$object] success!" : "Create object[$object] failed! Response: [" . $response->body . "] Logid[" . $response->header ["x-bs-request-id"] . "]", $opt );
    // return $response;
};
/**
 * 获取文件信息，发送的为HTTP HEAD请求，文件信息都在http response的header中，不会提取文件的内容
 * @param string $bucket (Required)
 * @param string $object (Required)
 * @param array $opt (Optional)
 * @throws BCS_Exception
 * @return BCS_ResponseCore
 */
BCS.prototype.get_object_info = function(bucket, object, opt, callback) {
    var opt = opt || {}; 
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['object'] = object;
    opt['method'] = 'HEAD';
    opt['callback'] = callback;
    this.authenticate(opt);
};
/**
 * 判断object是否存在
 * @param string $bucket (Required)
 * @param string $object (Required)
 * @param array $opt (Optional)
 * @throws BCS_Exception
 * @return boolean true false
 */
BCS.prototype.is_object_exist = function(bucket, object, opt, callback) {
    var opt = opt || {}; 
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['object'] = object;
    opt['method'] = 'HEAD';
    opt['callback'] = callback;
    this.get_object_info(bucket, object, opt, function(err, res) {
        if (err) {
            callback(err, res);
        } else {
            isOK(res.status)
            callback(err, isOK(res.status))
        }
    });
    /*
    $this->log ( $response->isOK () ? "Object exist!" : "Object may not exist! Response: [" . $response->body . "]", $opt );
    if ($response->isOK ()) {
        return true;
    } else {
        return false;
    }
    */
};
/**
 * 生成签名
 * @param array opt
 * @return boolean|string
 */
BCS.prototype.format_signature = function(opt) {
    var flags = "";
    var content = '';
    var shasum = crypto.createHmac('sha1', opt['sk']);
    var sign = '';
    if (!opt['ak'] || !opt['sk']) {
        console.log( 'ak or sk is not in the array when create factor!' );
        return false;
    }
    if ( (opt['bucket'] || opt.bucket === '') && opt ['method'] && opt['object']) {
        flags += 'MBO';
        content += "Method=" + opt['method'] + "\n"; //method
        content += "Bucket=" + opt['bucket'] + "\n"; //bucket
        content += "Object=" + path.normalize(opt['object']) + "\n"; //object
    } else {
        console.log( 'bucket、method and object cann`t be NULL!' );
        return false;
    }

    if (opt['ip']) {
        flags += 'I';
        content += "Ip=" + opt['ip'] + "\n";
    }
    if (opt['time']) {
        flags += 'T';
        content += "Time=" + opt['time'] + "\n";
    }
    if (opt['size']) {
        flags += 'S';
        content += "Size=" + opt['size'] + "\n";
    }
    content = flags + "\n" + content;
    
    shasum.update(content);
    sign = shasum.digest('base64');
    // sign = base64_encode ( hash_hmac ( 'sha1', $content, $opt [self::SK], true ) );
    return 'sign=' + flags + ':' + opt['ak'] + ':' + encodeURIComponent(sign);
};

/**
 * 构造url
 * @param array $opt
 * @return boolean|string
 */
// query_string
// { foo: 'bar', baz: ['qux', 'quux'], corge: '' }
BCS.prototype.format_url = function(opt) {
    var sign = this.format_signature(opt);
    if (sign === false) {
        console.log( "Format signature failed, please check!" );
        return false;
    }
    opt['sign'] = sign;
    var url = "";
    url += this.use_ssl ? 'https://' : 'http://';
    url += this.host;
    url += '/' + opt['bucket'];
    if ( opt['object'] && '/' !== opt['object'] ) {
        url += "/" + encodeURIComponent(opt['object']);
    }
    url += '?' + sign;
    if ( opt['query_string'] ) {
        url += '&' + querystring.stringify(opt['query_string']);
    }
    return url;
};

/**
 * 生成put_object的url
 * @param string $bucket (Required)
 * @param string $object (Required)
 * return false| string url
 */
BCS.prototype.generate_put_object_url = function(bucket, object, opt) {
    // opt array
    /*
    if (opt != NULL && ! is_array ( $opt )) {
        throw new BCS_Exception ( '$opt must be array, please check!', - 1 );
    }
    if (! $opt) {
        $opt = array ();
    }
    */
    var opt = opt || null;
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['method'] = 'PUT';
    opt['object'] = object;
    opt['query_string'] = {};
    if (opt["time"]) {
        opt['query_string']["time"] = opt["time"];
    }
    if (opt["size"]) {
        opt['query_string']["size"] = opt ["size"];
    }
    return this.format_url(opt);
};
/**
 * 生成post_object的url
 * @param string $bucket (Required)
 * @param string $object (Required)
 * return false| string url
 */
BCS.prototype.generate_post_object_url = function(bucket, object, opt) {
    //$opt array
    /*
    if ($opt != NULL && ! is_array ( $opt )) {
        throw new BCS_Exception ( '$opt must be array, please check!', - 1 );
    }
    if (! $opt) {
        $opt = array ();
    }
    */
    var opt = opt || null;
    opt['ak'] = this.ak;
    opt['sk'] = this.sk;
    opt['bucket'] = bucket;
    opt['method'] = 'POST';
    opt['object'] = object;
    opt['query_string'] = {};
    if (opt["time"]) {
        opt['query_string']["time"] = opt["time"];
    }
    if (opt["size"]) {
        opt['query_string']["size"] = opt ["size"];
    }
    return this.format_url(opt);
};
/**************************
    * Util Functin *
 **************************/
/**
 * @return the $use_ssl
 */
BCS.prototype.getUse_ssl = function() {
    return this.use_ssl;
};

/**
 * @param boolean $use_ssl
 */
BCS.prototype.setUse_ssl = function(use_ssl) {
    this.use_ssl = use_ssl;
};
/**
 * 将常用set http-header的动作抽离出来
 * @param string $header
 * @param string $value
 * @param array $opt
 * @throws BCS_Exception
 * @return void
 */
BCS.prototype.set_header_into_opt = function(header, value, opt) {
    /*
    if (isset ( $opt [self::HEADERS] )) {
        if (! is_array ( $opt [self::HEADERS] )) {
            trigger_error ( 'Invalid $opt[\'headers\'], please check.' );
            throw new BCS_Exception ( 'Invalid $opt[\'headers\'], please check.', - 1 );
        }
    } else {
        $opt [self::HEADERS] = array ();
    }
    $opt [self::HEADERS] [$header] = $value;
    */
    if(!opt['headers']) {
        opt['headers'] = {};
    }
    opt['headers'][header] = value;
    console.log(opt);
};
/**
 * 校验bucket是否合法，bucket规范
 * 1. 由小写字母，数字和横线'-'组成，长度为6~63位 
 * 2. 不能以数字作为Bucket开头 
 * 3. 不能以'-'作为Bucket的开头或者结尾
 * @param string $bucket
 * @return boolean
 */
BCS.prototype.validate_bucket = function(bucket) {
    //bucket 正则
    var pattern = /^[a-z][-a-z0-9]{4,61}[a-z0-9]$/;

    if ( (typeof bucket === 'string') && bucket.match(pattern) ) {
        return true;
    }
    console.log("Bucket Validate Failed:" + bucket);
    return false;
};

/**
 * 校验object是否合法，object命名规范
 * 1. object必须以'/'开头
 * @param string $object
 * @return boolean
 */
BCS.prototype.validate_object = function(object) {
    if ( (typeof object === 'string') && object.indexOf('/') === 0) {
        return true;
    }
    return false;
};

exports.BCS = BCS;