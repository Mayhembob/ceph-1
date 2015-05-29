#include <stdlib.h>
#include <string>
#include <sstream>
#include <iostream>
#include <boost/uuid/uuid.hpp>            
#include <boost/uuid/uuid_generators.hpp> 
#include <boost/uuid/uuid_io.hpp>         
#include "cls_tabular_ops.h"
#include "cls_tabular_client.h"
#include "include/types.h"
#include "include/rados/librados.hpp"

static void usage(const char *e)
{
  fprintf(stdout, "%s -p <pool> -t <table to monitor>\n", e);
}

int main(int argc, char **argv){
  std::string table_str, pool_str;
  std::vector<uint64_t> finished_splits;
  librados::Rados rados;
  librados::IoCtx ioctx;
  int ret;

  while (1) {
    int c = getopt(argc, argv, "p:t:");
    if (c == -1)
      break;
    switch (c) {
      case 'p':
        pool = std::string(optarg);
        break;
      case 't':
        table_str = std::string(optarg);
        break;
      default:
        usage(argv[0]);
        exit(1);
    }
  }
  
  open_ioctx(pool, rados, ioctx);
  
  // somehow check the table that was passed as 
  // a cmd line arg and see if it has any splits
  
  
  while (1) {
    for (int i = 0; i < table.splits.size(); i++) {
      std::string storage_obj = splits[i].oid;
      
      librados::ObjectWriteOperation op;
      cls_tabluar_get_split(op, finished_splits);
      ret = ioctx.operate(storage_obj, &op);
      assert(ret == 0);
      
      // assume the split object is inside 'split'
      
      // create a new storage object
      uuid = boost::uuids::random_generator()();
      uuid_ss.str("");
      uuid_ss << table.unique_id << "." << uuid;
      
      // need some way to write storage object to 
      // the table
      
      /*
      table_split new_split;
      new_split.oid = uuid_ss.str();
      new_split.lower_bound = split.min;
      new_split.upper_bound = split.max;
      */
      
      // write the range to the new object
      librados::ObjectWriteOperation op;
      cls_tabular_set_range(op, new_split.lower_bound, new_split.upper_bound);
      int ret = ioctx.operate(new_split.oid, &op);
      assert(ret == 0);
      
      // write the data to the new object
      std::vector<std::string> entries = split.data;
      cls_tabular_put(op, entries);
      
      // add the split id to the vector of finished splits
      finished_splits.push(split.id);

    }
  }
}
  
  
  