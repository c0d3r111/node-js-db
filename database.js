const zlib     = require('zlib');
const fs       = require('fs');
const notepack = require('notepack.io');

function Database(options) {
  this.location = options.location.endsWith('/') ? options.location : options.location + '/';
  this.location = this.location[0] === '/' ? this.location : '/' + this.location;
  this.store    = Object.create(null);
  this.readonly = options.readonly;

  if (options.keys) {
    for (let key of options.keys) {
      this.store[hash(key)] = Object.create(null);
    }
  }
}

Database.prototype.addkey    = function(name) {
  this.store[name] = Object.create(null);

  return;
};
Database.prototype.clear     = async function() {
  for (let key of Object.keys(this.store)) {
    this.store[key] = Object.create(null);
  }
  
  await Promise.all([
    this.save(),
    this.write(this.location + 'vault', Buffer.alloc(0))
  ]);

  return;
};
Database.prototype.cursor    = async function() {
  let location = this.location;
  let stats    = await new Promise(function(resolve) {
    void fs.stat(location + 'vault', function(e, s) {
      if (e) return void resolve(false);

      return void resolve(s);
    });
  });

  if (stats) {
    return stats.size === 0 ? 0 : stats.size + 1;
  }

  return 0;
};
Database.prototype.each      = async function(method) {
  // let method    = _method.bind(this);
  let positions = this.positions();
  let keys      = Object.freeze(Object.keys(positions));

  for (let position of keys) {
    await method(await this.get(position, positions[position]));
  }

  return;
};
Database.prototype.entry     = async function(data, size, pos) {
  let file = this.file;

  await new Promise(function(resolve) {
    void fs.write(file, data, 0, size >> 0, pos >> 0, resolve);
  });

  return;
};
Database.prototype.exists    = function(path) {
  return new Promise(function(resolve) {
    void fs.access(path, function(e) {
      if (e) return void resolve(false);
      
      return void resolve(true);
    });
  });
};
Database.prototype.find      = async function(query, limit = 10, offset = 0) {
  let keys    = Object.freeze(Object.keys(query));
  let ids     = Object.create(null);
  let len     = keys.length;

  for (let key of keys) {
    let entry = this.store[key];
    
    if (entry) {
      let value = entry[query[key]];

      if (value) {
        let positions = Object.freeze(Object.keys(value));

        for (let pos of positions) {
          if (ids[pos]) ids[pos].t += 1;
          else          ids[pos] = {t: 1, s: value[pos]};
        }
      }
    }
  }

  return await this.sort(ids, offset, limit, len);
};
Database.prototype.findgt    = async function(query, limit = 10, offset = 0, last) {
  let keys    = Object.freeze(Object.keys(query));
  let ids     = Object.create(null);
  let len     = keys.length;

  for (let key of keys) {
    let entry = this.store[key];
    
    if (entry) {
      let entries = Object.freeze(Object.keys(entry));
      let value   = query[key];
      let type    = typeof value;

      for (let current of entries) {
        let cast = type === 'string' ? current : current >> 0;

        if (cast >= query[key]) {
          let positions = Object.freeze(Object.keys(entry[current]));

          for (let pos of positions) {
            if (ids[pos]) ids[pos].t += 1;
            else          ids[pos] = {t: 1, s: entry[current][pos]};
          }
        }
      }
    }
  }

  return await this.sort(ids, offset, limit, len);
};
Database.prototype.findlt    = async function(query, limit = 10, offset = 0, last) {
  let keys    = Object.freeze(Object.keys(query));
  let ids     = Object.create(null);
  let len     = keys.length;

  for (let key of keys) {
    let entry = this.store[key];
    
    if (entry) {
      let entries = Object.freeze(Object.keys(entry));
      let value   = query[key];
      let type    = typeof value;

      for (let current of entries) {
        let cast = type === 'string' ? current : current >> 0;

        if (cast < query[key]) {
          let positions = Object.freeze(Object.keys(entry[current]));

          for (let pos of positions) {
            if (ids[pos]) ids[pos].t += 1;
            else          ids[pos] = {t: 1, s: entry[current][pos]};
          }
        }
      }
    }
  }

  return await this.sort(ids, offset, limit, len);
};
Database.prototype.get       = async function(pos, size) {
  let file = this.file;
  let data = await new Promise(function(resolve) {
      size = size >> 0;
      pos  = pos  >> 0;

    void fs.read(file, Buffer.alloc(size), 0, size, pos, function(_, _, d) {
      if (d) return void resolve(d);
      else   return void resolve(false);
    });
  });

  if (!data) return false;

  return await this.unzip(data);
};
Database.prototype.handle    = function(path) {
  return new Promise(function(resolve) {
    void fs.open(path, 'r+', function(e, fd) {
      if (e) throw 'Error getting vault file';

      return void resolve(fd);
    });
  });
};
Database.prototype.has       = function(query) {
  let keys  = Object.freeze(Object.keys(query));
  let len   = keys.length;

  for (let key of keys) {
    let value = query[key];
    let entry = this.store[key];

    if (entry && entry[value]) {
      len -= 1;
    }
  }

  if (len === 0) return true;

  return false;
};
Database.prototype.open      = async function() {
  if (!(await this.exists(this.location))) {
    let cmd      = require('child_process').exec;
    let location = this.location;

    await new Promise(function(resolve) {
      void cmd(`mkdir -p ${location}`, resolve);
    });
  }

  let [storebuffer, hasvault] = await Promise.all([
    this.read(this.location + 'store'),
    this.exists(this.location + 'vault'),
  ]);

  if (!hasvault) {
    await this.write(this.location + 'vault', Buffer.alloc(0));
  }
  
  this.file = await this.handle(this.location + 'vault');

  if (storebuffer) {
    let store = await this.unzip(storebuffer);

    if (store) this.store = store;
  }  
  else {
    void console.warn('Error opening saved files. Using fresh Database.');
  }

  if (!this.readonly) {
    void this.retain();
  }

  return;
};
Database.prototype.positions = function() {
  let keys  = Object.freeze(Object.keys(this.store));
  let data  = Object.create(null);

  for (let key of keys) {
    let entry      = this.store[key];
    let properties = Object.freeze(Object.keys(entry));

    for (let value of properties) {      
      let current   = entry[value];
      let positions = Object.freeze(Object.keys(current));

      for (let pos of positions) {
        data[pos] = current[pos];
      }
    }
  }

  return Object.freeze(data);
};
Database.prototype.read      = function(path) {
  return new Promise(function(resolve) {
    void fs.readFile(path, function(e, d) {
      if (e) return void resolve(false);
      else   return void resolve(d);
    });
  })
};
Database.prototype.remove    = async function(query) {
  let keys    = Object.freeze(Object.keys(query));
  let ids     = Object.create(null);
  let len     = keys.length;

  for (let key of keys) {
    let entry = this.store[key]
    
    if (!entry) continue;

    let value = entry[query[key]];

    if (!value) continue;
    
    let positions = Object.freeze(Object.keys(value));

    for (let pos of positions) {
      if (ids[pos]) ids[pos].t += 1;
      else          ids[pos] = {t: 1, s: value[pos]};
    }
  }

  let matches = Object.freeze(Object.keys(ids));

  for (let pos of matches) {
    let current = ids[pos];

    if (current.t === len) {      
      await this.entry(Buffer.alloc(current.s), current.s, pos);
    }
  }

  return;
};
Database.prototype.removekey = function(name) {
  delete this.store[name];
};
Database.prototype.retain    = async function() {
  await new Promise(function(resolve) {
    void setTimeout(resolve, 5e3);
  });
  await this.save();

  return void this.retain();
};
Database.prototype.save      = async function() {
  if (this.readonly) throw 'Error! Attempting to save to readonly database.';

  let store = await this.zip(this.store);
  
  await this.write(this.location + 'store', store);

  return;
};
Database.prototype.set       = async function(data) {
  let keys        = Object.freeze(Object.keys(data));
  let [seal, pos] = await Promise.all([
    this.zip(data),
    this.cursor(),
  ]);
  let size        = Buffer.byteLength(seal);
  
  for (let key of keys) {
    let entry   = this.store[key];
    
    if (!entry) continue;

    let value   = data[key];

    if (entry[value]) {
      entry[value][pos] = size;
    }
    else {
      entry[value]      = Object.create(null);
      entry[value][pos] = size;
    }

    this.store[key] = entry;
  }

  await this.entry(seal, size, pos);

  return;
};
Database.prototype.select    = async function(query) {
  let keys    = Object.freeze(Object.keys(query));
  let ids     = Object.create(null);
  let len     = keys.length;

  for (let key of keys) {
    let entry = this.store[key];
    
    if (entry) {
      let value = entry[query[key]];

      if (value) {
        let positions = Object.freeze(Object.keys(value));

        for (let pos of positions) {
          if (ids[pos]) ids[pos].t += 1;
          else          ids[pos] = {t: 1, s: value[pos]};
        }
      }
    }
  }

  let results = Object.freeze(Object.keys(ids));
  let matches = [];

  for (let pos of results) {
    if (ids[pos].t === len) {
      void matches.push(pos);
    }
  }

  let max = Math.max(...matches);

  return ids[max] ? await this.get(max, ids[max].s) : false;
};
Database.prototype.size      = function() {
  let keys  = Object.freeze(Object.keys(this.store));
  let total = new Set();

  for (let key of keys) {
    let entry = this.store[key];
    let props = Object.freeze(Object.keys(entry));

    for (let prop of props) {      
      Object.keys(entry[prop]).forEach(k => void total.add(k));
    }
  }

  return total.size;
};
Database.prototype.sort      = async function(ids, offset, limit, len) {
  let results = [];
  let matches = Object.freeze(Object.keys(ids).slice(offset));

  for (let pos of matches) {
    if (ids[pos].t === len) {
      let data = await this.get(pos, ids[pos].s);

      if (data)                    void results.push(data);
      if (results.length >= limit) break;
    }
  }
  
  return results.length > 1 ? results : results[0];
}
Database.prototype.unzip     = async function(data) {
  return new Promise(function (resolve) {
    void zlib.inflate(data, function (e, buffer) {
      if (e) return void resolve(false);
      
      return void resolve(notepack.decode(buffer));
    });
  });
};
Database.prototype.update    = async function(query, data) {
  let keys    = Object.freeze(Object.keys(query));

  if (keys.length > 1) throw "Error attempting to update a multi-key entry.";

  let current = await this.select(query);

  if (current) {
    let key   = keys[0];
    let value = query[key];

    this.store[key][value] = Object.create(null);

    await this.set({...current, ...data});
  }

  return;
};
Database.prototype.write     = function(path, data) {
  return new Promise(function(resolve) {
    void fs.writeFile(path, data, resolve);
  });
};
Database.prototype.zip       = async function(data) {
  let entry = Buffer.isBuffer(data) ? data : notepack.encode(data);

  return new Promise(function (resolve) {
    void zlib.deflate(entry, { level: 5 }, function (e, buffer) {
      if (e) resolve(false);
      else   resolve(buffer);

    });
  });
};


module.exports = Database;
