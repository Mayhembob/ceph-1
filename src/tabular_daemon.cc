#include <stdlib.h>
#include <string>
#include <sstream>
#include <iostream>
#include "include/types.h"
#include "include/rados/librados.hpp"

static void usage(const char *e)
{
  fprintf(stdout, "%s -t <name of table to monitor>\n", e);
}

int main(int argc, char **argv){
  std:string table_str;
  
  while (1) {
    int c = getopt(argc, argv, "t:");
    if (c == -1)
      break;
    switch (c) {
      case 't':
        table_str = std::string(optarg);
        break;
      default:
        usage(argv[0]);
        exit(1);
    }
  }
  
  // what is the type of the table? How do we convert
  // the string to the table type?
  something? table = new cls_tabular(table_str);
  
  while (1) {
    for (int i = 0; i < table.splits.size(); i++) {
      std::string storage_obj_str = splits[i].oid;
      something? storage_obj = ...
      
      // if there are pending splits
      if (storage_obj.split_points.size() != 0) {
        storage_obj.cls_tabluar_get_split();
      }
        
    }
  }
}
  
  
  