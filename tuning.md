## Tokyo Tyrant

`*` - in-memory hash
`+` - in-memory b-tree
`.tch` - on-disk hash
`.tcb` - on-disk b-tree
`.tcf` - fixed-length database
`.tct` - table database

In-memory hash:

* `bnum` - number of buckets
* `capnum` - capacity number of records
* `capsiz` - capacity size of memory, when exceeded removed via FIFO.

In-memory b-tree:

* `capnum` - capacity number of records
* `capsiz` - capacity size of memory, when exceeded removed via FIFO.

On-disk hash:

* `mode` - w=writer, r=reader, c=create, t=truncate, e=nolock default=wc
* `bnum` - main tuning parameter, should be > number of records, default=131071
* `apow` - alignment power, default=4, so 2^4 = 16
* `fpow` - maximum elements in free block pool, default=10, so 2^10=1024
* `opts` - l=large (>2gb), d=default, b=bzip2, t=tcbs
* `rcnum` - record cache, default=0 (disabled)
* `xmsiz` - extra mapped memory region size, default is 67108864 (64mb)
* `dfunit` - unit step for auto-defragmentation, default=0 (disabled).

On-disk b-tree:

* `mode`
* `lmemb` - number of members in each leaf node, default=128
* `nmemb` - number of members in each non-leaf node, default=256
* `bnum` - main tuning parameter, > 1/128 number of records, default=32749
* `apow` - alignment power, default=8, so 2^8=256
* `fpow` - maximum elements in free block pool, default=10, so 2^10=1024
* `opts` - l=large (>2gb), d=default, b=bzip2, t=tcbs
* `lcnum` - main tuning parameter, default is 1024, leaf-node cache size
* `ncnum` - non-leaf nodes to cache, default=512
* `xmsiz` - extra mapped memory region size, default=0 (disabled)
* `dfunit` - unit step for auto-defragmentation, default=0 (disabled).

Fixed-length:

* `mode`
* `width` - width of each value, default=255
* `limsiz` - max size of database, default is 268435456 (256mb)

Table:

* `mode`
* `bnum` - default is 131071, should be 0.5 - 4x number of records
* `apow` - record alignment power, default=4, 2^4=16
* `fpow` - maximum elements in free block pool, default=10, so 2^10=1024
* `opts` - l=large (>2gb), d=default, b=bzip2, t=tcbs
* `rcnum` - number of records to cache, default=0 (disabled)
* `lcnum` - leaf-nodes to cache, default=4096
* `ncnum` - non-leaf nodes to cache, default=512
* `xmsiz` - extra mapped memory region size, default is 67108864 (64mb)
* `dfunit` - unit step for auto-defragmentation, default=0 (disabled).
* `idx` - field-name and type, separated by ":"


## Kyoto Tycoon

Supported by all databases:

* `log` - path to logfile
* `logkinds` - debug, info, warn or error
* `logpx` - prefix for each log message

Other options:

* `bnum` - number of buckets in the hash table
* `capcnt` - set capacity by record number
* `capsiz` - set capacity by memory usage
* `psiz` - page size
* `pccap` - page-cache capacity
* `opts` - s=small, l=linear, c=compress - l=linked-list for hash collisions.
* `zcomp` - compression algorithm: zlib, def (deflate), gz, lzo, lzma or arc
* `zkey` - cipher key for compression (?)
* `rcomp` - comparator function: lex, dec (decimal), lexdesc, decdesc
* `apow` - alignment power of 2
* `fpow` - maximum elements in the free-block pool
* `msiz` - tune map
* `dfunit` - unit step for auto-defragmentation, default=0 (disabled).

Databases and available parameters:

* Stash - bnum
* Cache hash (`*`): opts, bnum, zcomp, capcnt, capsiz, zkey
* Cache tree (`%`): opts, bnum, zcomp, zkey, psiz, rcomp, pccap
* File hash (`.kch`): opts, bnum, apow, fpow, msiz, dfunit, zcomp, zkey
* File tree (`.kct`): opts, bnum, apow, fpow, msiz, dfunit, zcomp, zkey, psiz,
                      rcomp, pccap
* Dir hash: (`.kcd`): opts, zcomp, zkey
* Dir tree: (`.kcf`): opts, zcomp, zkey, psiz, rcomp, pccap
* Plain-text: (`.kcx`): n/a

#### Stash database

* `bnum` - default is ~1M, should be 80%-400% of total records

#### CacheHashDB

* `bnum`: default ~1M. Should be 50% - 400% of total records. Collision
  chaining is binary search
* `opts`: useful to reduce memory at expense of time effciency. Use compression
  if the key and value of each record is greater-than 1KB
* `capcnt` and/or `capsiz`: keep memory usage constant by expiring old records

#### CacheTreeDB

Inherits all tuning options from the CacheHashDB, since each node of the btree
is serialized as a page-buffer and treated as a record in the cache hash db.

* `psiz`: default is 8192
* `pccap`: default is 64MB
* `rcomp`: default is lexical ordering

#### HashDB

* `bnum`: default ~1M. Suggested ratio is twice the total number of records,
  but can be anything from 100% - 400%.
* `apow`: Power of the alignment of record size. Default=3, so the address of
  each record is aligned to a multiple of 8 (`2^3`) bytes.
* `fpow`: Power of the capacity of the free block pool. Default=10, rarely
  needs to be modified.
* `msiz`: Size of internal memory-mapped region. Default is 64MB.
* `dfunit`: Unit step number of auto-defragmentation. Auto-defrag is disabled
  by default.

apow, fpow, opts and bnum *must* be specified before a DB is opened and
cannot be changed after the fact.

#### TreeDB

Inherits tuning parameters from the HashDB.

* `psiz`: default is 8192, specified before opening db and cannot be changed
* `pccap`: default is 64MB
* `rcomp`: default is lexical

The default alignment is 256 (2^8) and the default bucket number is ~64K.
The bucket number should be calculated by the number of pages. Suggested
ratio of bucket number is 10% of the number of records.
