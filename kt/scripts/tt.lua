-- Find records whose key matches a pattern. Accepts pattern and optional
-- maximum number of results.
function match_pattern(key, value)
  value = tonumber(value)
  if not value then value = 0 end
  local res = ""
  function proc(tkey, tvalue)
    if string.match(tkey, key) then
      res = res .. tkey .. "\t" .. tvalue .. "\n"
      value = value - 1
      if value == 0 then return false end
    end
    return true
  end
  _foreach(proc)
  return res
end


-- Find records whose key is within a certain edit distance. Accepts pattern
-- key and edit distance, which defaults to 0 (exact match) if not provided.
function match_similar(key, value)
  value = tonumber(value)
  if not value then
    value = 0
  end
  local res = ""
  function proc(tkey, tvalue)
    if _dist(tkey, key) <= value then
      res = res .. tkey .. "\t" .. tvalue .. "\n"
    end
    return true
  end
  _foreach(proc)
  return res
end


-- Find records whose value is within a certain edit distance. Accepts value
-- pattern and edit distance.
function match_similar_value(key, value)
  value = tonumber(value)
  if not value then
    value = 0
  end
  local res = ""
  function proc(tkey, tvalue)
    if _dist(tvalue, key) <= value then
      res = res .. tkey .. "\t" .. tvalue .. "\n"
    end
    return true
  end
  _foreach(proc)
  return res
end


-- Lock a key.
function lock(key, value)
  if _lock(key) then return "true" else return "false" end
end

-- Unlock a key.
function unlock(key, value)
  if _unlock(key) then return "true" else return "false" end
end

-- Seize/pop implementation.
function seize(key, value)
  _lock(key)
  local res = _get(key) or ""
  if res ~= nil then _out(key) end
  _unlock(key)
  return res
end


-- De-serialize and serialize a table database value to a lua table.
function mapload(m)
  local t = {}
  local elems = _split(m)
  for i = 1, #elems, 2 do
    t[elems[i]] = elems[i + 1]
  end
  return t
end


function mapdump(t)
  local res = ""
  local glue = string.char(0)
  local key, value
  for key, value in pairs(t) do
    res = res .. key .. glue .. value .. glue
  end
  return res
end


-- Table helpers for working with values in a table database.
function table_get(key, value)
  local tval = _get(key)
  if tval ~= nil then
    local tmap = mapload(tval)
    return tmap[value] or ''
  end
  return ''
end

function table_update(key, value)
  -- Value is assumed to be NULL-separated key/value.
  local items = _split(value)
  if #items < 2 then
    _log('expected null-separated key/value pairs for for table_set()')
  else
    local tval = _get(key)
    local tmap
    if tval == nil then
      tmap = {}
    else
      tmap = mapload(tval)
    end
    for i = 1, #items, 2 do
      tmap[items[i]] = items[i + 1]
    end
    _put(key, mapdump(tmap))
    return "true"
  end
  return "false"
end

function table_pop(key, value)
  local tval = _get(key)
  if tval ~= nil then
    local tmap = mapload(tval)
    local ret = tmap[value]
    if ret ~= nil then
      tmap[value] = nil
      _put(key, mapdump(tmap))
    end
    return ret or ''
  end
  return ''
end


-- Split a string.
function split(key, value)
  if key == "" then return "" end

  if #value < 1 then
    value = nil
  end
  local elems = _split(key, value)
  local res = ""
  for i = 1, #elems do
    res = res .. elems[i] .. "\n"
  end
  return res
end


-- e.g. hash('foo bar', 'md5'), hash('checksum me', 'crc32')
function hash(key, value)
  if #value < 1 then value = "md5" end
  return _hash(value, key)
end

-- hash the value stored in a key, if it exists.
function hash_key(key, value)
  local tval = _get(key)
  if tval == nil then return '' end
  if #value < 1 then value = "md5" end
  return _hash(value, tval)
end


function time(key, value)
  return string.format("%.6f", _time())
end


function ptime(key, value)
  _log("current time: " .. _time())
  return "ok"
end


function getdate(key, value)
  -- Verify os module is available.
  return os.date("%Y-%m-%dT%H:%M:%S")
end


function glob(key, value)
  local paths = _glob(key)
  local res = ""
  for i = 1, #paths do
    res = res .. paths[i] .. "\n"
  end
  return res
end


-- Evaluate arbitrary user script.
function script(key, value)
  if not _eval(key) then
    return nil
  end
  return "ok"
end


-- Queue
-- enqueue a record
function queue_add(key, value)
  local id = _adddouble(key, 1)
  if not id then
    _log("unable to determine id")
    return nil
  end
  key = string.format("%s\t%012d", key, id)
  if not _putkeep(key, value) then
    _log("could not add key")
    return nil
  end
  return "ok"
end


-- dequeue a record
function queue_pop(key, max)
  max = tonumber(max)
  if not max or max < 1 then
    max = 1
  end
  key = string.format("%s\t", key)
  local keys = _fwmkeys(key, max)
  local res = ""
  for i = 1, #keys do
    local key = keys[i]
    local value = _get(key)
    if _out(key) and value then
      res = res .. value .. "\n"
    end
  end
  return res
end


-- blocking dequeue.
function queue_bpop(key, max)
  res = queue_pop(key, max)
  while res == "" do
    sleep(0.1)
    res = queue_pop(key, max)
  end
  return res
end


-- get the queue size
function queue_size(key)
  key = string.format("%s\t", key)
  local keys = _fwmkeys(key)
  return #keys
end


-- clear queue
function queue_clear(key)
  key = string.format("%s\t", key)
  local keys = _fwmkeys(key)
  _misc("outlist", unpack(keys))
  _out(key)
  return #keys
end
