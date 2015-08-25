#ifndef PYTHON_PLUGIN_H
#define PYTHON_PLUGIN_H

#include <Python.h> // First, to avoid warnings

#include <string>
#include "arc/Logger.h"

namespace ARex {

  class PythonPlugin {
  private:
    PyObject *py_func;
    PyObject *py_mod;
    static Arc::Logger logger;
  public:
    PythonPlugin(void);
    bool init(std::string module, std::string function);
    void call(void *arg);
    ~PythonPlugin(void);
  };
}


#endif
