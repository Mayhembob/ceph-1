#ifndef CEPH_CLS_TABULAR_OPS_H
#define CEPH_CLS_TABULAR_OPS_H

#include "include/types.h"
#include "cls_tabular_types.h"

struct cls_tabular_put_op {
  vector<string> entries;

  cls_tabular_put_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_tabular_put_op)

struct cls_tabular_set_range_op {
  uint64_t min;
  uint64_t max;

  cls_tabular_set_range_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(min, bl);
    ::encode(max, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(min, bl);
    ::decode(max, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_tabular_set_range_op)

#endif
