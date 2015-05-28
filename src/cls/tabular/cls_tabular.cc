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
  uint64_t lower;
  uint64_t upper;
};

struct split_with_data {
  split_range split;
  bufferedlist data;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(split, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }
  
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(split, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
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
  uint64_t total_entries;      // total entries, including non-gc entries
  uint64_t effective_entries;  // entries in a valid range for this object

  // range we are accepting
  uint64_t lower_bound;
  uint64_t upper_bound;

  uint64_t lower_bound_seen;
  uint64_t upper_bound_seen;
  
  bool split_required;

  std::vector<split_range> split_points;
  std::vector<split_range> splits_in_progress;

  cls_tabular_header() {
    lower_bound = 0;
    upper_bound = (uint64_t)-1;
    total_entries = 0;
    effective_entries = 0;
    split_required = false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(total_entries, bl);
    ::encode(effective_entries, bl);
    ::encode(lower_bound, bl);
    ::encode(upper_bound, bl);
    ::encode(lower_bound_seen, bl);
    ::encode(upper_bound_seen, bl);
    ::encode(split_points, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(total_entries, bl);
    ::decode(effective_entries, bl);
    ::decode(lower_bound, bl);
    ::decode(upper_bound, bl);
    ::decode(lower_bound_seen, bl);
    ::decode(upper_bound_seen, bl);
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
    
  split_range split = split_points.pop();  
  splits_in_progress.push(split);
  
  // read data in the range 
  bufferlist data;
  ret = cls_cxx_read(hctx, split.lower, split.upper, data);
  
  split_with_data split_data;
  split_data.data = data;
  split_data.split = split;
    
  out->append(split_data);
    
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

    // initialize 'seen' bounds
    if (header.total_entries == 0) {
      header.lower_bound_seen = key_val;
      header.upper_bound_seen = key_val;
    }

    header.total_entries++;
    header.effective_entries++;

    // update 'seen' bounds
    if (key_val < header.lower_bound_seen)
      header.lower_bound_seen = key_val;
    if (key_val > header.upper_bound_seen)
      header.upper_bound_seen = key_val;
  }

  // insert the entries into our database. we know at this point that they are
  // all valid for the range associated with this object.
  ret = cls_cxx_map_set_vals(hctx, &entries);
  if (ret < 0) {
    CLS_ERR("ERROR: cls_tabular_put: failed to write entries");
    return ret;
  }

<<<<<<< Updated upstream
  // if the number of entries in the range that we are accepting exceeds 1000
  // then we create a split.
  // 
  // We want to reject future writes by updating 'lower bound' and 'upper
  // bound'. And also want to update the seen bounds for the new valid range.
  //
  // Insert split point into the object header metadata.
  //
  if (header.effective_entries > 1000) {
    uint64_t split_point = header.lower_bound_seen + ((header.upper_bound_seen -  header.lower_bound_seen) / 2);
    split_required = true;
    CLS_ERR("cls_tabular_put: split: entries %lu lower %lu upper %lu split %lu\n",
        header.effective_entries,
        header.lower_bound_seen,
        header.upper_bound_seen,
        split_point);
=======
  if (header.entries > 1000) {
    split_range split;
    split.upper =
      header.lower_bound + ((header.upper_bound - header.lower_bound) / 2);
    split.lower = header.lower_bound;
    CLS_ERR("split created: lb=%llu up=%llu sp=%llu entries=%d\n",
        header.lower_bound, header.upper_bound, split_point, header.entries);
    header.lower_bound = split_point;
    header.split_points.push_back(split);
    header.entries = 0;
>>>>>>> Stashed changes
  }

  ret = write_header(hctx, header);
  if (ret < 0) {
    CLS_ERR("cls_tabular_put: failed to write header");
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
}
