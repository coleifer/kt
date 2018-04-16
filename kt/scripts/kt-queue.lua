kt = __kyototycoon__
db = kt.db

-- Queue

-- enqueue data
-- accepts { queue, data[, db] }
-- returns { id }
function enqueue(inmap, outmap)
  local db = getdb(inmap)
  local queue = inmap.queue
  local data = inmap.data
  if not queue or not data then
    return kt.RVEINVALID
  end
  local id = db:increment_double(queue, 1)
  if not id then
    kt.log("info", "unable to determine id")
    return kt.RVELOGIC
  end
  key = string.format("%s\t%012d", queue, id)
  if not db:add(key, data) then
    kt.log("info", "could not add key, already exists")
    return kt.RVELOGIC
  end
  outmap.id = id
  return kt.RVSUCCESS
end

-- dequeue data
-- accepts { queue, n }
-- returns { idx=data }
function dequeue(inmap, outmap)
  local db = getdb(inmap)
  local queue = inmap.queue
  if not queue then
    return kt.RVEINVALID
  end

  local n = inmap.n
  if n then
    n = tonumber(n)
  else
    n = 1
  end

  local key = string.format("%s\t", queue)
  local keys = db:match_prefix(key, n)
  for i = 1, #keys do
    local k = keys[i]
    local val = db:get(k)
    if db:remove(k) and val then
      outmap[tostring(i - 1)] = val
    end
  end
  return kt.RVSUCCESS
end


-- queue size
-- accepts { queue }
-- returns { num }
function queuesize(inmap, outmap)
  local db = getdb(inmap)
  local queue = inmap.queue
  if not queue then
    return kt.RVEINVALID
  end
  local keys = db:match_prefix(string.format("%s\t", queue))
  outmap.num = tostring(#keys)
  return kt.RVSUCCESS
end


function getdb(inmap)
  local dbidx = inmap.db
  if dbidx then
    return kt.dbs[tonumber(dbidx) + 1]
  end
  return kt.db
end
