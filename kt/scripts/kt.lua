kt = __kyototycoon__
db = kt.db


-- helper function for hash functions.
function hkv(inmap, outmap, fn)
  local key = inmap.table_key
  if not key then
    return kt.RVEINVALID
  end
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
    o.num = #v
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
    return kt.RVEINVALID
  end
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
    o.num = #v
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
    return kt.RVEINVALID
  end
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


-- Misc helpers.


-- Move src to dest.
-- accepts: { src, dest }
-- returns: {}
function move(inmap, outmap)
  local src = inmap.src
  local dest = inmap.dest
  if not src or not dest then
    return kt.RVEINVALID
  end
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
  local cur = db:cursor()
  cur:jump()
  while true do
    local key, value, xt = cur:get(true)
    if not key then break end
    outmap[key] = value
  end
  return kt.RVSUCCESS
end
