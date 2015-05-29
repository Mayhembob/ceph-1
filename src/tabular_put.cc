#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <iostream>
#include <random>
#include <thread>
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>
#include <mutex>
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular_client.h"

/*
 *  * Convert value into zero-padded string for omap comparisons.
 *   */
static inline std::string u64tostr(uint64_t value)
{
  std::stringstream ss;
  ss << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}

/*
 * Convert string into numeric value.
 */
static inline int strtou64(string value, uint64_t *out)
{
  uint64_t v;
 
  try {
    v = boost::lexical_cast<uint64_t>(value);
  } catch (boost::bad_lexical_cast &) {
    return -EIO;
  }
 
  *out = v;
  return 0;
}


/*
 * This represents a partition of a table.
 *
 * oid:  name of the object holding the data for this partition.
 *
 * TODO: a table split needs to have a range.
 *        encode/decode the range.
 */
struct table_split {
  std::string oid;
  uint64_t lower_bound;
  uint64_t upper_bound;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(oid, bl);
    ::encode(lower_bound, bl);
    ::encode(upper_bound, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(oid, bl);
    ::decode(lower_bound, bl);
    ::decode(upper_bound, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(table_split)

/*
 * This represents the metadata for a table.
 *
 * seq:
 * unique_id:
 * splits:     this is the set of partitions of the table
 */
struct table_state {
  int seq;
  std::string unique_id;
  std::vector<table_split> splits;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(unique_id, bl);
    ::encode(splits, bl);
    ::encode(seq, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(unique_id, bl);
    ::decode(splits, bl);
    ::decode(seq, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(table_state)

/*
 * This client interface to the table.
 */
struct table {
  librados::IoCtx& ioctx;
  std::string head;
  table_state state;

  /*
   * poll for new splits. this should be replaced by watch/notify.
   */
  std::thread *watch;
  void watch_func() {
    while (1) {
      sleep(1);

      bufferlist bl;
      int ret = ioctx.read(head, bl, 0, 0);
      assert(ret > 0);

      librados::bufferlist::iterator iter = bl.begin();

      table_state new_state;
      ::decode(new_state, iter);

      if (state.seq == new_state.seq) {
        for (unsigned i = 0; i < new_state.splits.size(); i++) {
          std::cout << "table " << head << ": split " << i << ": " << new_state.splits[i].oid << std::endl;
        }
        state = new_state;
      }
    }
  }

  /*
   * Create new table instance. The "head" parameter is the name of the table
   * (or more precisely the name of the object containing the metadata for the
   * table).
   */
  table(librados::IoCtx& ioctx, std::string head) :
    ioctx(ioctx), head(head) {

      // read the metadata out of the head object
      bufferlist bl;
      int ret = ioctx.read(head, bl, 0, 0);
      assert(ret > 0);

      // decode the table_state that was read.
      librados::bufferlist::iterator iter = bl.begin();
      ::decode(state, iter);

      // debugging: print out splits
      for (unsigned i = 0; i < state.splits.size(); i++) {
        std::cout << "table " << head << ": split " << i << ": " << state.splits[i].oid << std::endl;
      }

      // ???
      watch = new std::thread(&table::watch_func, *this);
  }

  int put(std::string entry) {
    std::vector<std::string> entries;
    entries.push_back(entry);
    return put(entries);
  }

  /*
   * put a set of entries into object
   */
  int put(std::vector<std::string>& entries) {
    assert(entries.size() == 1);

    // we've hard coded into this for now the assumption that entries vector
    // only ever contains one entry that we handle at a time.
    uint64_t val;
    int ret = strtou64(entries[0], &val);
    assert(ret == 0);

    bool simulated_split = false;

    while (1) {

      librados::ObjectWriteOperation op;
      cls_tabular_put(op, entries);

      // figure out which split owns this entry
      bool target_found = false;
      table_split target_split;
      for (std::vector<table_split>::const_iterator it = state.splits.begin();
          it != state.splits.end(); it++) {
        const table_split& split = *it;
        if (split.lower_bound <= val && val <= split.upper_bound) {
          target_split = split;
          target_found = true;
          break;
        }
      }
      assert(target_found);

      int ret = ioctx.operate(target_split.oid, &op);
      if (ret < 0) {
        if (ret != -ERANGE) {
          fprintf(stderr, "ret=%d e=%s\n", ret, strerror(-ret));
          fflush(stderr);
          exit(1);
        }

        fprintf(stdout, "tring again...\n");
        fflush(stderr);
        sleep(1);

        if (!simulated_split) {
          simulate_split();
          simulated_split = true;
        }

        continue;
      }
      return 0;
    }
  }

  /*
   * The splitting daemon isn't completed yet, so we need a way to simulate
   * splits. The dumbest thing to do is just to split everything when we see
   * that one object was overloaded. We also don't do any data copying or
   * clean-up.
   */
  void simulate_split() {

    std::vector<table_split> new_splits;

    assert(state.splits.size() > 0);

    for (std::vector<table_split>::const_iterator it = state.splits.begin();
        it != state.splits.end(); it++) {

      const table_split& split = *it;

      uint64_t split_point =
        split.lower_bound + ((split.upper_bound - split.lower_bound) / 2);

      // new split
      boost::uuids::uuid uuid = boost::uuids::random_generator()();
      std::stringstream uuid_ss;
      uuid_ss << uuid;

      table_split new_split;
      new_split.oid = uuid_ss.str();
      new_split.lower_bound = split.lower_bound;
      new_split.upper_bound = split_point;
      new_splits.push_back(new_split);

      librados::ObjectWriteOperation op;
      cls_tabular_set_range(op, new_split.lower_bound, new_split.upper_bound);
      int ret = ioctx.operate(new_split.oid, &op);
      assert(ret == 0);

      // modified original
      table_split orig_split = split;
      orig_split.lower_bound = split_point;
      new_splits.push_back(orig_split);
    }

    state.splits = new_splits;
    bufferlist bl;
    ::encode(state, bl);
    int ret = ioctx.write_full(head, bl);
    assert(ret == 0);
  }
};

static void open_ioctx(std::string &pool, librados::Rados &rados, librados::IoCtx &ioctx)
{
  int ret = rados.init(NULL);
  assert(ret == 0);

  rados.conf_read_file(NULL);
  assert(ret == 0);

  rados.connect();
  assert(ret == 0);

  ret = rados.ioctx_create(pool.c_str(), ioctx);
  assert(ret == 0);
}

static void usage(const char *e)
{
  fprintf(stdout, "%s -p pool\n", e);
}

void daemon_func(table& table) {
  table_state state = table.state;
  while (1) {
    sleep(1);
    
    // need to update table state here
    
    // how to get the state? should the head of the table
    // be passed as an argument to daemon_func()?      
    // what should be the type of storage_object?
    // what should be the types of header and error?
    
    for (unsigned i = 0; i < state.splits.size(); i++) {
      // how exactly to access the storage object?
      storage_object = state.splits[i].oid; 
      
      // getting the header from the storage object.
      storage_object.omap_get_header(header, error);
      
      // if the split_required flag is set, then create a new
      // object and copy over part of the keys.
      if(header.split_required) {
        
        // create a new object and add it to the table
        uuid = boost::uuids::random_generator()();
        uuid_ss.str("");
        uuid_ss << state.unique_id << "." << uuid;

        table_split split;
        split.oid = uuid_ss.str(); // the oid of the new object
        
        state.splits.push_back(split);

        // update table metadata to include new object
        bufferlist bl;
        ::encode(state, bl);
        int ret = ioctx.write_full(table, bl);
        assert(ret == 0);
        
        // copy over the data
        map<std::string, bufferlist> data;
        uint64_t last_split = split_points.back();
        std::string last_split_str = utostr(last_split);
        
        storage_object.omap_get_vals(last_split_str
        1000,
        data,
        error);
        
        
        
        
        // let the old object know it can delete the range that
        // has been copied over.
        
        // split has been completed, reset the flag
        storage_object.header.split_required = false;
      }
    }
  }    
}

int main(int argc, char **argv)
{
  std::string pool, objname;
  bool create = false;
  bool splitter = false;
  bool daemon_enabled = false;

  while (1) {
    int c = getopt(argc, argv, "p:o:csd");
    if (c == -1)
      break;
    switch (c) {
      case 'p':
        pool = std::string(optarg);
        break;
      case 'o':
        objname = std::string(optarg);
        break;
      case 'c':
        create = true;
        break;
      case 's':
        splitter = true;
        break;
      case 'd':
        daemon_enabled = true;
        break;
      default:
        usage(argv[0]);
        exit(1);
    }
  }

  fprintf(stdout, "conf: objname=%s\n",
      objname.c_str());

  // connect to rados
  librados::Rados rados;
  librados::IoCtx ioctx;
  open_ioctx(pool, rados, ioctx);

  /*
   * Create a new "table" when the user provides the "-c" option.
   */
  if (create) {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::stringstream uuid_ss;
    uuid_ss << uuid;

    /*
     * The object name is provided by the user with the "-o" option. This will
     * remove any object with the provided name, and then create a empty
     * object with the same name.
     */
    ioctx.remove(objname);
    ioctx.create(objname, true);

    /*
     * Construct the metadata that describes the table as it is stored within
     * RADOS. The table state holds the set of splits, but since this is the
     * very first instance of this metadata for this table it will just be in
     * an initialized state.
     */
    table_state s;
    s.seq = 0;
    s.unique_id = uuid_ss.str();

    /*
     * Create the first partition which will have -inf,+inf. We will name the
     * object holding the first parition with a random string, but we will
     * keep track of all these names in our metadata.
     */

    // first we create the name
    uuid = boost::uuids::random_generator()();
    uuid_ss.str("");
    uuid_ss << s.unique_id << "." << uuid;

    // create the split
    table_split split;
    split.oid = uuid_ss.str();
    split.lower_bound = 0;
    split.upper_bound = UINT64_MAX;


    // add the split to the table
    s.splits.push_back(split);

    // write the metadata (table state) into the object that represents the
    // table (i.e. inode).
    bufferlist bl;
    ::encode(s, bl);
    int ret = ioctx.write_full(objname, bl);
    assert(ret == 0);

    // now create the first object and initialize its bounds to be unbounded
    librados::ObjectWriteOperation op;
    cls_tabular_set_range(op, split.lower_bound, split.upper_bound);
    ret = ioctx.operate(split.oid, &op);
    assert(ret == 0);

    return 0;
  }
  
  /*
   * This daemon is responsible for monitoring the objects, which it
   * gets access to through table_split. When an object needs a split
   * the daemon will create a new object and copy over half of the old 
   * objects range. 
   */
  if (daemon_enabled) {
      std::thread daemon_thread(daemon_func);
  }

  /*
   * We'll be inserting key/value pairs that are (int, bytes). And we'll use a
   * uniform random distribution for the key.
   */
  std::default_random_engine generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 1ULL<<32);

  /*
   * This represents the client's view of the table and provides the interface
   * to interact with the table.
   */
  table t(ioctx, objname);

  /*
   * Generate load on the table by inserting key/value pairs.
   *
   * TODO: notice that we are adding just the keys. This is fine for now, but
   * later we'll want to have a byte array follow around the keys as a value
   * in a key/value pair.
   */
  while (1) {
    // create an empty "batch" of things to insert into the table.
    std::vector<std::string> entries;

    // add an entry to the batch
    uint64_t key = distribution(generator);
    entries.push_back(u64tostr(key));

    // add the batch to the table
    int ret = t.put(entries);
    assert(ret == 0);
  }

  ioctx.close();
  rados.shutdown();

  return 0;
}