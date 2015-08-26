#ifndef PYTHON_PLUGIN_H
#define PYTHON_PLUGIN_H

#include <string>
#include "arc/Logger.h"

#ifndef Py_PYTHON_H
class PyObject;
#endif

namespace Arc {
class JobDescription;
}

namespace ARex {

  class PythonPlugin {
  private:
    PyObject* py_func;
    PyObject* py_mod;
    PyObject* py_lrms;
    static Arc::Logger logger;
  public:
    PythonPlugin(void);
    bool init(std::string submit_module);
    void submit(Arc::JobDescription* arg);
    ~PythonPlugin(void);
  };
}


#endif
