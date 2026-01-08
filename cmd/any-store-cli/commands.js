function DB() {}

DB.prototype.createCollection = function (name) {
    return {
        result: function () {
            return JSON.stringify({
                cmd: 'createCollection',
                collection: name,
            })
        }
    }
}

DB.prototype.backup = function (path) {
    return {
        result: function () {
            return JSON.stringify({
                cmd: 'backup',
                path: path,
            })
        }
    }
}

function Query(name) {
    this.collection = name;
    this.cmd = "find";
    this.query = {};
}

Query.prototype.limit = function (limit) {
    this.query.limit = limit;
    return this
}

Query.prototype.offset = function (offset) {
    this.query.offset = offset;
    return this
}

Query.prototype.sort = function (limit) {
    this.query.sort = Array.prototype.slice.call(arguments);
    return this
}

Query.prototype.hint = function (hint) {
    this.query.hint = hint;
    return this
}

Query.prototype.project = function (project) {
    this.query.project = project;
    return this
}

Query.prototype.pretty = function () {
    this.query.pretty = true;
    return this
}

Query.prototype.count = function () {
    this.query.count = true;
    var res = JSON.stringify(this);
    this.query = {};
    return {
        result: function () {
            return res
        }
    }
}

Query.prototype.explain = function () {
    this.query.explain = true;
    var res = JSON.stringify(this);
    this.query = {};
    return {
        result: function () {
            return res
        }
    }
}

Query.prototype.delete = function () {
    this.query.delete = true;
    var res = JSON.stringify(this);
    this.query = {};
    return {
        result: function () {
            return res
        }
    }
}

Query.prototype.update = function (upd) {
    this.query.update = upd || {};
    var res = JSON.stringify(this);
    this.query = {};
    return {
        result: function () {
            return res
        }
    }
}

Query.prototype.result = function () {
    var res = JSON.stringify(this);
    this.query = {};
    return res
}


function Collection(name) {
    this.collection = name;
    this.index = {};
    this.query = new Query(name);
}

Collection.prototype.find = function (condition) {
    this.query.query.find = condition || {}
    return this.query;
}

Collection.prototype.findOne = function (condition) {
    this.query.query.find = condition || {}
    this.query.query.limit = 1
    this.query.cmd = "findOne"
    var res = JSON.stringify(this.query);
    this.query.cmd = "find";
    this.query.query = {
        find: {},
    };
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.count = function () {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "count",
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.ensureIndex = function (index) {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "ensureIndex",
        index: index,
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.dropIndex = function (indexName) {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "dropIndex",
        index: {name:indexName},
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.drop = function () {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "drop",
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.insert = function () {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "insert",
        documents: Array.prototype.slice.call(arguments)
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.upsert = function () {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "upsert",
        documents: Array.prototype.slice.call(arguments)
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.update = function () {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "update",
        documents: Array.prototype.slice.call(arguments)
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.deleteId = function () {
    var res = JSON.stringify({
        collection: this.collection,
        cmd: "deleteId",
        documents: Array.prototype.slice.call(arguments)
    });
    return {
        result: function () {
            return res
        }
    }
}

Collection.prototype.findId = function () {
    this.query.cmd = "findId"
    this.query.documents = Array.prototype.slice.call(arguments)
    return this.query
}

var db = new DB();
