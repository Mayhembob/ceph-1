#ifndef CLS_TABULAR_CLIENT_H
#define CLS_TABULAR_CLIENT_H

#include <vector>
#include <string>

#include "include/rados/librados.hpp"

void cls_tabular_put(librados::ObjectWriteOperation& op,
    std::vector<std::string>& entries);

void cls_tabular_set_range(librados::ObjectWriteOperation& op,
    uint64_t min, uint64_t max);
#endif
