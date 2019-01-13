kt = __kyototycoon__
db = kt.db


function _select_db(inmap)
  if inmap.db then
    db_idx = tonumber(inmap.db) + 1
    inmap.db = nil
    db = kt.dbs[db_idx]
  else
    db = kt.db
  end
  return db
end

-- helper function for hash functions.
function hkv(inmap, outmap, fn)
  local key = inmap.table_key
  if not key then
    kt.log("system", "hash function missing required: 'table_key'")
    return kt.RVEINVALID
  end
  _select_db(inmap) -- Allow db to be specified as argument.
  inmap.table_key = nil
  local value, xt = db:get(key)
  local value_tbl = {}
  if value then
    value_tbl = kt.mapload(value)
  end
  local new_value, ok = fn(key, value_tbl, inmap, outmap)
  if ok then
    if new_value and not db:set(key, kt.mapdump(new_value), xt) then
      return kt.RVEINTERNAL
    else
      return kt.RVSUCCESS
    end
  else
    return kt.RVELOGIC
  end
end

-- Redis-like HMSET functionality for setting multiple key/value pairs.
-- accepts: { table_key, ... }
-- returns: { num }
function hmset(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value, num
    num = 0
    for key, value in pairs(i) do
      v[key] = value
      num = num + 1
    end
    o.num = num
    return v, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HMGET functionality for getting multiple key/value pairs.
-- accepts: { table_key, k1, k2 ... }
-- returns: { k1=v1, k2=v2, ... }
function hmget(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value
    for key, value in pairs(i) do
      o[key] = v[key]
    end
    return nil, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HMDEL functionality for deleting multiple key/value pairs.
-- accepts: { table_key, k1, k2 ... }
-- returns: { num }
function hmdel(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value, num
    num = 0
    for key, value in pairs(i) do
      if v[key] then
        num = num + 1
        v[key] = nil
      end
    end
    o.num = num
    return v, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HGETALL functionality for getting entire contents of hash.
-- accepts: { table_key }
-- returns: { k1=v1, k2=v2, ... }
function hgetall(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value
    for key, value in pairs(v) do
      o[key] = value
    end
    return nil, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HSET functionality for setting a single key/value in a hash.
-- accepts: { table_key, key, value }
-- returns: { num }
function hset(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value = i.key, i.value
    if not key or not value then
      return nil, false
    end
    v[key] = value
    o.num = 1
    return v, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HSET functionality for setting a key/value if key != exist.
-- accepts: { table_key, key, value }
-- returns: { num }
function hsetnx(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value = i.key, i.value
    if not key or not value then
      return nil, false
    end
    if v[key] ~= nil then
      o.num = 0
      return nil, true
    else
      v[key] = value
      o.num = 1
      return v, true
    end
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HGET functionality for getting a single key/value in a hash.
-- accepts: { table_key, key }
-- returns: { value }
function hget(inmap, outmap)
  local fn = function(k, v, i, o)
    local key = i.key
    if not key then
      return nil, false
    end
    o.value = v[key]
    return nil, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HDEL functionality for deleting a single key/value in a hash.
-- accepts: { table_key, key }
-- returns: { num }
function hdel(inmap, outmap)
  local fn = function(k, v, i, o)
    local key = i.key
    if not key then
      return nil, false
    end
    if v[key] then
      v[key] = nil
      o.num = 1
    else
      o.num = 0
    end
    return v, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HLEN functionality for determining number of items in a hash.
-- accepts: { table_key }
-- returns: { num }
function hlen(inmap, outmap)
  local fn = function(k, v, i, o)
    local count = 0
    for _ in pairs(v) do
      count = count + 1
    end
    o.num = count
    return nil, true
  end
  return hkv(inmap, outmap, fn)
end


-- Redis-like HCONTAINS functionality for determining if key exists in a hash.
-- accepts: { table_key, key }
-- returns: { num }
function hcontains(inmap, outmap)
  local fn = function(k, v, i, o)
    local key = i.key
    if not key then
      return nil, false
    end
    if v[key] then
      o.num = 1
    else
      o.num = 0
    end
    return nil, true
  end
  return hkv(inmap, outmap, fn)
end


-- helper function for set functions.
function skv(inmap, outmap, fn)
  local key = inmap.key
  if not key then
    kt.log("system", "set function missing required: 'key'")
    return kt.RVEINVALID
  end
  _select_db(inmap) -- Allow db to be specified as argument.
  inmap.key = nil
  local value, xt = db:get(key)
  local value_tbl = {}
  if value then
    value_tbl = kt.mapload(value)
  end
  local new_value, ok = fn(key, value_tbl, inmap, outmap)
  if ok then
    if new_value and not db:set(key, kt.mapdump(new_value), xt) then
      return kt.RVEINTERNAL
    else
      return kt.RVSUCCESS
    end
  else
    return kt.RVELOGIC
  end
end


-- Redis-like SADD functionality for adding value/score to set.
-- accepts: { key, value } where multiple values are delimited by '\x01'
-- returns: { num }
function sadd(inmap, outmap)
  local fn = function(k, v, i, o)
    local value = i.value
    if not value then
      return nil, false
    end
    local n = 0
    local values = kt.split(value, "\1")
    for i = 1, #values do
      if v[values[i]] == nil then
        v[values[i]] = ""
        n = n + 1
      end
    end
    outmap.num = n
    if n == 0 then
      return nil, true
    else
      return v, true
    end
  end
  return skv(inmap, outmap, fn)
end


-- Redis-like SCARD functionality for determining cardinality of a set.
-- accepts: { key }
-- returns: { num }
function scard(inmap, outmap)
  local fn = function(k, v, i, o)
    local count = 0
    for _ in pairs(v) do
      count = count + 1
    end
    o.num = count
    return nil, true
  end
  return skv(inmap, outmap, fn)
end


-- Redis-like SISMEMBER functionality for determining if value in a set.
-- accepts: { key, value }
-- returns: { num }
function sismember(inmap, outmap)
  local fn = function(k, v, i, o)
    local value = i.value
    if not value then
      return nil, false
    end

    o.num = 0
    if v[value] ~= nil then
      o.num = 1
    end
    return nil, true
  end
  return skv(inmap, outmap, fn)
end


-- Redis-like SMEMBERS functionality for getting all values in a set.
-- accepts: { key }
-- returns: { v1, v2, ... }
function smembers(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value
    for key, value in pairs(v) do
      o[key] = '1'
    end
    return nil, true
  end
  return skv(inmap, outmap, fn)
end


-- Redis-like SPOP functionality for removing a member from a set.
-- accepts: { key }
-- returns: { num, value }
function spop(inmap, outmap)
  local fn = function(k, v, i, o)
    local key, value
    o.num = 0
    for key, value in pairs(v) do
      o.num = 1
      o.value = key
      v[key] = nil
      return v, true
    end
    return nil, true
  end
  return skv(inmap, outmap, fn)
end


-- Redis-like SREM functionality for removing a value from a set.
-- accepts: { key, value }
-- returns: { num }
function srem(inmap, outmap)
  local fn = function(k, v, i, o)
    local value = i.value
    if not value then
      return nil, false
    end

    local n = 0
    local values = kt.split(value, "\1")
    for i = 1, #values do
      if v[values[i]] ~= nil then
        n = n + 1
        v[values[i]] = nil
      end
    end
    o.num = n
    if n > 0 then
      return v, true
    else
      return nil, true
    end
  end
  return skv(inmap, outmap, fn)
end


-- helper function for set operations on 2 keys.
function svv(inmap, outmap, fn)
  local key1, key2 = inmap.key1, inmap.key2
  if not key1 or not key2 then
    kt.log("system", "set function missing required: 'key1' or 'key2'")
    return kt.RVEINVALID
  end
  _select_db(inmap) -- Allow db to be specified as argument.
  local value1, xt = db:get(key1)
  local value2, xt = db:get(key2)

  local value_tbl1 = {}
  local value_tbl2 = {}
  if value1 then value_tbl1 = kt.mapload(value1) end
  if value2 then value_tbl2 = kt.mapload(value2) end

  local ret = fn(value_tbl1, value_tbl2, inmap, outmap)
  if ret == kt.RVSUCCESS and inmap.dest then
    if not db:set(inmap.dest, kt.mapdump(outmap), xt) then
      return kt.RVEINTERNAL
    end
  end
  return kt.RVSUCCESS
end


-- Redis-like SINTER functionality for finding intersection of 2 sets.
-- accepts: { key1, key2, (dest) }
-- returns: { ... }
function sinter(inmap, outmap)
  local fn = function(v1, v2, i, o)
    local key, val
    for key, val in pairs(v1) do
      if v2[key] ~= nil then
        o[key] = '1'
      end
    end
    return kt.RVSUCCESS
  end
  return svv(inmap, outmap, fn)
end


-- Redis-like SUNION functionality for finding union of 2 sets.
-- accepts: { key1, key2, (dest) }
-- returns: { ... }
function sunion(inmap, outmap)
  local fn = function(v1, v2, i, o)
    local key, val
    for key, val in pairs(v1) do
      o[key] = '1'
    end
    for key, val in pairs(v2) do
      o[key] = '1'
    end
    return kt.RVSUCCESS
  end
  return svv(inmap, outmap, fn)
end


-- Redis-like SDIFF functionality for finding difference of set1 and set2.
-- accepts: { key1, key2, (dest) }
-- returns: { ... }
function sdiff(inmap, outmap)
  local fn = function(v1, v2, i, o)
    local key, val
    for key, val in pairs(v1) do
      if v2[key] == nil then
        o[key] = '1'
      end
    end
    return kt.RVSUCCESS
  end
  return svv(inmap, outmap, fn)
end


-- helper function for list functions.
function lkv(inmap, outmap, fn)
  local key = inmap.key
  if not key then
    kt.log("system", "list function missing required: 'key'")
    return kt.RVEINVALID
  end
  _select_db(inmap) -- Allow db to be specified as argument.
  inmap.key = nil
  local value, xt = db:get(key)
  local value_array = {}
  if value then
    value_array = kt.arrayload(value)
  end
  local new_value, ok = fn(key, value_array, inmap, outmap)
  if ok then
    if new_value and not db:set(key, kt.arraydump(new_value), xt) then
      return kt.RVEINTERNAL
    else
      return kt.RVSUCCESS
    end
  else
    return kt.RVELOGIC
  end
end


-- Redis-like LPUSH
-- accepts: { key, value }
-- returns: {}
function llpush(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    local value = inmap.value
    if not value then
      kt.log("system", "missing value parameter to llpush")
      return nil, false
    end
    table.insert(arr, 1, value)
    return arr, true
  end
  return lkv(inmap, outmap, fn)
end

-- Redis-like RPUSH
-- accepts: { key, value }
-- returns: {}
function lrpush(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    local value = inmap.value
    if not value then
      kt.log("system", "missing value parameter to lrpush")
      return nil, false
    end
    table.insert(arr, value)
    return arr, true
  end
  return lkv(inmap, outmap, fn)
end


function _normalize_index(array_len, idx)
  local index = tonumber(idx or "0") + 1
  if index < 1 then
    index = array_len + index
    if index < 1 then return nil, false end
  end
  if index > array_len then return nil, false end
  return index, true
end


-- Redis-like LRANGE -- zero-based.
-- accepts: { key, start, stop }
-- returns: { i1, i2, ... }
function lrange(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    local arrsize = #arr
    local start = tonumber(inmap.start or "0") + 1
    if start < 1 then
      start = arrsize + start
      if start < 1 then
        return nil, true
      end
    end

    local stop = inmap.stop
    if stop then
      stop = tonumber(stop)
      if stop < 0 then
        stop = arrsize + stop
      end
    else
      stop = arrsize
    end

    for i = start, stop do
      outmap[i - 1] = arr[i]
    end
    return nil, true
  end
  return lkv(inmap, outmap, fn)
end


-- Redis-like LINDEX -- zero-based.
-- accepts: { key, index }
-- returns: { value }
function lindex(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    local index, ok = _normalize_index(#arr, inmap.index)
    if ok then
      local val = arr[index]
      outmap.value = arr[index]
    end
    return nil, true
  end
  return lkv(inmap, outmap, fn)
end


-- LINSERT -- zero-based.
-- accepts: { key, index, value }
-- returns: {}
function linsert(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    local index, ok = _normalize_index(#arr, inmap.index)
    if not ok then
      return nil, false
    end
    if not inmap.value then
      kt.log("info", "missing value for linsert")
      return nil, false
    end
    table.insert(arr, index, inmap.value)
    return arr, true
  end
  return lkv(inmap, outmap, fn)
end


-- Redis-like LPOP -- removes first elem.
-- accepts: { key }
-- returns: { value }
function llpop(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    outmap.value = arr[1]
    table.remove(arr, 1)
    return arr, true
  end
  return lkv(inmap, outmap, fn)
end


-- Redis-like RPOP -- removes last elem.
-- accepts: { key }
-- returns: { value }
function lrpop(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    outmap.value = arr[#arr]
    arr[#arr] = nil
    return arr, true
  end
  return lkv(inmap, outmap, fn)
end


-- Redis-like LLEN -- returns length of list.
-- accepts: { key }
-- returns: { num }
function llen(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    outmap.num = #arr
    return nil, true
  end
  return lkv(inmap, outmap, fn)
end


-- Redis-like LSET -- set item at index.
-- accepts: { key, index, value }
-- returns: {}
function lset(inmap, outmap)
  local fn = function(key, arr, inmap, outmap)
    local idx = tonumber(inmap.index or "0")
    if not inmap.value then
      kt.log("info", "missing value for lset")
      return nil, false
    end
    if idx < 0 or idx >= #arr then
      kt.log("info", "invalid index for lset")
      return nil, false
    end
    arr[idx + 1] = inmap.value
    return arr, true
  end
  return lkv(inmap, outmap, fn)
end


-- Misc helpers.


-- Move src to dest.
-- accepts: { src, dest }
-- returns: {}
function move(inmap, outmap)
  local src = inmap.src
  local dest = inmap.dest
  if not src or not dest then
    kt.log("info", "missing src and/or dest key in move() call")
    return kt.RVEINVALID
  end
  _select_db(inmap) -- Allow db to be specified as argument.
  local keys = { src, dest }
  local first = true
  local src_val = nil
  local src_xt = nil
  local function visit(key, value, xt)
    -- Operating on first key, capture value and xt and remove.
    if first then
      src_val = value
      src_xt = xt
      first = false
      return kt.Visitor.REMOVE
    end

    -- Operating on dest key, store value and xt.
    if src_val then
      return src_val, src_xt
    end
    return kt.Visitor.NOP
  end

  if not db:accept_bulk(keys, visit) then
    return kt.REINTERNAL
  end

  if not src_val then
    return kt.RVELOGIC
  end
  return kt.RVSUCCESS
end


-- List all key-value pairs.
-- accepts: {}
-- returns: { k=v ... }
function list(inmap, outmap)
  _select_db(inmap) -- Allow db to be specified as argument.
  local cur = db:cursor()
  cur:jump()
  while true do
    local key, value, xt = cur:get(true)
    if not key then break end
    outmap[key] = value
  end
  cur:disable()
  return kt.RVSUCCESS
end


-- Fetch a range of key-value pairs.
-- accepts: { start: key, stop: key, db: idx }
-- returns: { k1=v1, k2=v2, ... }
function get_range(inmap, outmap)
  _select_db(inmap)
  local start_key = inmap.start
  local stop_key = inmap.stop
  local cur = db:cursor()
  if start_key then
    if not cur:jump(start_key) then
      cur:disable()
      return kt.RVSUCCESS
    end
  else
    if not cur:jump() then
      cur:disable()
      return kt.RVSUCCESS
    end
  end
  local key, value
  while true do
    key = cur:get_key()
    if stop_key and key > stop_key then
      break
    end
    outmap[key] = cur:get_value()
    if not cur:step() then
      break
    end
  end
  cur:disable()
  return kt.RVSUCCESS
end


-- Hash one or more values.
-- accepts: { val1: method1, val2: method2, ... }
-- returns: { val1: hash1, val2: hash2, ... }
function hash(inmap, outmap)
  local key, value
  for key, val in pairs(inmap) do
    if val == 'fnv' then
      outmap[key] = kt.hash_fnv(val)
    else
      outmap[key] = kt.hash_murmur(val)
    end
  end
end
