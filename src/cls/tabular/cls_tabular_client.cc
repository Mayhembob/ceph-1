#include "include/types.h"
#include "cls/tabular/cls_tabular_ops.h"
#include "cls/tabular/cls_tabular_client.h"
#include "include/rados/librados.hpp"

/*
 * Take a set of entries and prepare the operation such that when it is
 * executed it calls the "put" method on the "tabular" class.
 */
void cls_tabular_put(librados::ObjectWriteOperation& op,
    std::vector<std::string>& entries)
{
  librados::bufferlist in;
  cls_tabular_put_op call;
  call.entries = entries;
  ::encode(call, in);
  op.exec("tabular", "put", in);
}

void cls_tabular_set_range(librados::ObjectWriteOperation& op,
    uint64_t min, uint64_t max)
{
  librados::bufferlist in;
  cls_tabular_set_range_op call;
  call.min = min;
  call.max = max;
  ::encode(call, in);
  op.exec("tabular", "set_range", in);
}
