# quark
Storage engine for minimimal databases.

## Intention
We often found ourselves needing a datastore that has tunable durability, great
write performance, great range scan performance and with great interface to our
existing codebase.

Our usecase is timeseries events, which means insertion is constantly almost
worst case for tree-based indexes. The closest solution we found was Cassandra,
but it features a client heavy protocol to implement in librcd and it's runtime
stability does not seem to be all that.

## Tunable Durability
We use the "acid memory" implementation in librcd to get an interface that looks
like mmap:ed files that have performant and known durability by two-phase
commiting pages.

## Memory allocator
This project features a minimal memory allocator that looks somewhat like
dlalloc. However it only allocates blocks of form PAGE_SIZE *2^n, free
requires knowledge of the allocation size because there is no allocation
headers.

## The almost B-skip-list
While SST:s is almost impossible to beat for range scan performance implementing
them with performant insertion is very hard. Cassandras mem-tables and
performance quirks are clear proof of that. Trees are horrible for ordered
insert. So we took inspiration from "The B-Skip-List: A Simpler Uniquely
Represented Alternative to B-Trees" by Daniel Golovin.

### Structure
Our skip list has a predetermined height, all levels of lists except the lowest
one are "index lists". "Higher" levels contain less keys than lower ones. The
bottom layer is the data layer, this layer is the only one that has every key
inserted. It also contains the value associated with the key.

### Partitions
A partition is a continuous block of memory. 
Partitions form doubly linked lists for all levels. The partitions are only a
supportive structure to the nodes to make disk access efficient.

### Nodes
In an ordinary skip-list the nodes would more or less be the data structure.
However because of the partitions form the dynamically linked structure our
nodes don't have to point to each other. They are simply packed as tightly
as possible in the available ranges in our partitions.

In the index layer they look like this:

    | uint64_t key_length | uint8_t[] key | partition* down |

And in the data layer:
    | uint64_t key_length | uint8_t[] key || uint64_t value_length | uint8_t[] value|

### Insert
When inserting a node into the skip-list first we must roll a level, we do this
exactly like D.Golovin suggests. At the insertion level we start searching
for the correct partition to place our node. This is done by iterating over the
partitions until the next pointer is zero or the key of the first node in the
next partition is larger than the key of the node being inserted. This partition
is referred to as the representative partition for this node. In the
representative partition we first check if it has space for our new node, if it
does we move all nodes with a larger key than the insert node up to make space
for the new node, then we insert the node. If it does not have space we instead
make space by creating a new partition of sufficient size and move all the nodes
with a larger key there, leaving space at the bottom for the new node. If this
was an index level the node contains a down pointer, to know what this pointer
will be we have to recursively insert at the layer below until the data level is
reached.

All levels below the insertion level must be split on insertion so that the key
of the insertion node is the first node in its partition, this is same
behavior as when a partition runs out of space.

If a partition is "split" because the node was bigger than the partition there
would be an empty partition in front of the new large one which serves no
purpose so they are de-allocated.

### Lookup
We define lookup as finding the representative partition for the node with the
desired key, this means that it will always succeed as long as the skip-list has
at least one partition.

Lookup works like in a B-skip-list.
