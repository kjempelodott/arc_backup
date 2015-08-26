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
    PyObject* py_submit;
    PyObject* py_cancel;
    PyObject* py_mod;
    PyObject* py_lrms;
    static Arc::Logger logger;
  public:
    PythonPlugin(void);
    bool init(const std::string& submit_module);
    bool submit(Arc::JobDescription* arg);
    bool cancel(const std::string& localid);
    ~PythonPlugin(void);
  };
}


#endif
