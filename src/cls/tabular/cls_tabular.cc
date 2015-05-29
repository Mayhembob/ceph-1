#include <errno.h>
#include <boost/lexical_cast.hpp>

#include "include/types.h"
#include "objclass/objclass.h"

#include "cls_tabular_types.h"
#include "cls_tabular_ops.h"

CLS_VER(1,0)
CLS_NAME(tabular)

cls_handle_t h_class;
cls_method_handle_t h_tabular_put;
cls_method_handle_t h_tabular_set_range;

/*
 * Convert string into numeric value.
 */
static inline int strtou64(string value, uint64_t *out)
{
  uint64_t v;
 
  try {
    v = boost::lexical_cast<uint64_t>(value);
  } catch (boost::bad_lexical_cast &) {
    CLS_ERR("converting key into numeric value %s", value.c_str());
    return -EIO;
  }
 
  *out = v;
  return 0;
}

struct split_range {
  uint64_t min;
  uint64_t max;
}

/*
 * It's hard to track how many rows we actually have because we can't
 * distinguish between writes to new rows and row updates. To simplify things
 * we assume that there aren't a significant number of updates. The key part
 * here is that leveldb doesn't provide a "count" interface.
 *
 * upper and lower bound controlling which range of keys we are managing and
 * can recieve operations. boolean values can be used to indicate when the
 * ends of the range are unbounded. We'll initially use integer keys, so using
 * actual min / max values for uint64 will work.
 *
 * min and max keys that have been seen. we'll assume uniform distribution, so
 * splitting is easy.
 */
struct cls_tabular_header {
  uint64_t entries;
  uint64_t lower_bound;
  uint64_t upper_bound;
  bool initialized;

  std::vector<split_range> split_points;

  cls_tabular_header() {
    initialized = false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(lower_bound, bl);
    ::encode(upper_bound, bl);
    ::encode(initialized, bl);
    ::encode(split_points, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    ::decode(lower_bound, bl);
    ::decode(upper_bound, bl);
    ::decode(initialized, bl);
    ::decode(split_points, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_tabular_header)

static int read_header(cls_method_context_t hctx, cls_tabular_header& header)
{
  bufferlist header_bl;

  int ret = cls_cxx_map_read_header(hctx, &header_bl);
  if (ret < 0)
    return ret;

  if (header_bl.length() == 0) {
    header = cls_tabular_header();
    return 0;
  }

  bufferlist::iterator iter = header_bl.begin();
  try {
    ::decode(header, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: read_header(): failed to decode header");
  }

  return 0;
}

static int write_header(cls_method_context_t hctx, cls_tabular_header& header)
{
  bufferlist header_bl;
  ::encode(header, header_bl);

  int ret = cls_cxx_map_write_header(hctx, &header_bl);
  if (ret < 0)
    return ret;

  return 0;
}

/*
 * This interface allows a daemon to read a random split from 
 * this object.
 */
static int cls_tabluar_get_split(cls_method_context_t hctx, 
bufferlist *in, bufferlist *out) {
  bool match;
  int fresh_split_index = -1;
  
  // loop to check for a split that has not been 
  // computed by the daemon before
  for (i = 0; i < split_points.size(); i++) {
    match = false;
    for (j = 0; j < finished_splits.size(); j++) {
      if (i == finished_splits[j]){
        match = true;
        break;
      }
    }
    if (match == false) {
      fresh_split_index = i;
      break;
    }
  }  
  
  // all the splits have already been taken care of
  // no more work needs to be done
  if (fresh_split_index == -1) {
    return -1;
  }
  
  // If a fresh split (fresh meaning it is in the 
  // header and the daemon has not worked on it yet)
  // has been found, then we want to work on that 
  // split range
  split_range split = split_points[fresh_split_index];  
  cls_tabular_get_split_op split_ret;
  
  uint64_t size;
  int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;

  if (size == 0)
    return -ENOENT;
  
  // read data in the range 
  bufferlist bl_data;
  ret = cls_cxx_read(hctx, 0, size, &bl_data);
  
  std::map<std::string, ceph::bufferedlist> data;
  try {
    bufferlist::iterator iter = bl_data.begin();
    ::decode(data, iter);
  } catch (const buffer::error &err) {
    return -EIO;
  }
  
  // the split we return to the daemon will have a min, max
  // an id (this is just the position of the split in the 
  // vector, which is unique since the vector is append only)
  // and the data. 
  split_ret.min = split.min;
  split_ret.max = split.max;
  split_ret.id  = fresh_split_index;
  
  // we have read all the data in the object, and now
  // we need to trim it to only the data in the range
  // specified by the split. 
  std::map<std::string, ceph::bufferedlist>::iterator min_it;
  std::map<std::string, ceph::bufferedlist>::iterator max_it;
  
  min_it = data.begin();
  max_it = data.find(split.min);
  data.erase(min_it, max_it);
  min_it = data.find(split.max);
  max_it = data.end();
  data.erase(min_it, max_it);
  
  split_ret.data = data;
    
  ::encode(split_ret, *out);
  
  return 0;
}
 
/*
 * This is how new entries are added to this object (partition).
 */
static int cls_tabular_put(cls_method_context_t hctx,
    bufferlist *in, bufferlist *out)
{
  /*
   * Decode the input parameters for this operation.
   */
  cls_tabular_put_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_ERR("ERROR: cls_tabluar_put: failed to decode op");
    return -EINVAL;
  }

  /*
   * Read the metadata for this object.
   */
  cls_tabular_header header;
  int ret = read_header(hctx, header);
  if (ret < 0) {
    CLS_ERR("ERROR: cls_tabular_put: failed to read header");
    return ret;
  }

  /*
   * Partitions are initialized using the cls_tabular_set_range method.
   */
  if (!header.initialized) {
    CLS_ERR("ERROR: objects must be initialized");
    return -EINVAL;
  }

  /*
   * Entries is what is going to go into the key/value store. Effectively the
   * set of entries we are going to accept. Currently we don't accept partial
   * puts.
   */
  map<string, bufferlist> entries;

  // for each entry sent by the client
  for (vector<string>::iterator it = op.entries.begin(); it != op.entries.end(); it++) {
    
    std::string& key = *it;

    // key/value pair with empty byte array
    entries[key] = bufferlist();

    // convert str(key) back to numeric value
    uint64_t key_val;
    int ret = strtou64(*it, &key_val);
    if (ret < 0)
      return ret;

    // if we are outside the bounds being accepted by this object's partition
    // range then return -ERANGE.
    if (key_val < header.lower_bound || key_val > header.upper_bound) {
      CLS_ERR("ERROR: cls_tabular_put: out of range");
      return -ERANGE;
    }

    header.entries++;
  }

  // insert the entries into our database. we know at this point that they are
  // all valid for the range associated with this object.
  ret = cls_cxx_map_set_vals(hctx, &entries);
  if (ret < 0) {
    CLS_ERR("ERROR: cls_tabular_put: failed to write entries");
    return ret;
  }

  if (header.entries > 1000) {
    uint64_t split_point =
      header.lower_bound + ((header.upper_bound - header.lower_bound) / 2);
    CLS_ERR("split created: lb=%llu up=%llu sp=%llu entries=%d\n",
        header.lower_bound, header.upper_bound, split_point, header.entries);
    header.lower_bound = split_point;
    header.split_points.push_back(split_point);
    header.entries = 0;
  }

  ret = write_header(hctx, header);
  if (ret < 0) {
    CLS_ERR("cls_tabular_put: failed to write header");
    return ret;
  }

  return 0;
}

static int cls_tabular_set_range(cls_method_context_t hctx,
    bufferlist *in, bufferlist *out)
{
  cls_tabular_set_range_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_ERR("ERROR: cls_tabluar_set_range: failed to decode op");
    return -EINVAL;
  }

  /*
   * We assume that a range can be set only on an object that doesn't exist,
   * meaning that we don't want to change the range on any object that is
   * already holding data. This is used to set the range for a new table's
   * first object, or could be used to initialize a fixed set of partitions.
   * This could be relaxed later if there was a need.
   */
  int ret = cls_cxx_stat(hctx, NULL, NULL);
  if (ret < 0 && ret != -ENOENT) {
    CLS_ERR("ERROR: cls_tabular_set_range: failed to stat %d", ret);
    return ret;
  }

  cls_tabular_header header;
  ret = read_header(hctx, header);
  if (ret < 0) {
    CLS_ERR("ERROR: cls_tabular_set_range: failed to read header %d", ret);
    return ret;
  }

  // what is being accepted
  header.lower_bound = op.min;
  header.upper_bound = op.max;
  header.entries = 0;
  header.initialized = true;

  ret = write_header(hctx, header);
  if (ret < 0) {
    CLS_ERR("ERROR: cls_tabular_set_range: failed to write header %d", ret);
    return ret;
  }

  return 0;
}

void __cls_init()
{
  CLS_LOG(1, "Loaded tabular class!");

  cls_register("tabular", &h_class);

  cls_register_cxx_method(h_class, "put",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_tabular_put, &h_tabular_put);

  cls_register_cxx_method(h_class, "set_range",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_tabular_set_range, &h_tabular_set_range);
      
  cls_register_cxx_method(h_class, "get_split",
      CLS_METHOD_RD,
      cls_tabular_get_split, &h_tabular_get_split);
}
