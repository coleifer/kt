-- Find records whose key matches a pattern.
function match_pattern(key, value)
  local res = ""
  function proc(tkey, tvalue)
    if string.match(tkey, key) then
      res = res .. tkey .. "\t" .. tvalue .. "\n"
    end
    return true
  end
  _foreach(proc)
  return res
end


-- Find records whose key is within a certain edit distance.
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


-- Find records whose value is within a certain edit distance.
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


-- e.g. hash('md5', 'foo bar'), hash('crc32', 'checksum me')
function hash(key, value)
  return _hash(key, value)
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


-- Queue
-- enqueue a record
function enqueue(key, value)
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
function dequeue(key, max)
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


-- get the queue size
function queuesize(key)
  key = string.format("%s\t", key)
  local keys = _fwmkeys(key)
  return #keys
end
