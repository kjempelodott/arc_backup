#include "Python.h"

#include "PythonPlugin.h"
#include "arc/ArcLocation.h"

namespace ARex {

  Arc::Logger PythonPlugin::logger(Arc::Logger::getRootLogger(), "PythonPlugin");

  PythonPlugin::PythonPlugin(void) : py_submit(NULL), py_cancel(NULL), py_mod(NULL), py_lrms(NULL) {}

  bool PythonPlugin::init(const std::string& lrms) {

    Py_Initialize();

    // Import the 'imp' library with its find_module and load_module functions
    PyObject* py_imp_str = PyString_FromString("imp");
    PyObject* py_imp_handle = PyImport_Import(py_imp_str);
    Py_DECREF(py_imp_str);

    PyObject* py_imp_dict = PyModule_GetDict(py_imp_handle);
    PyObject* py_imp_find_module = PyDict_GetItemString(py_imp_dict, "find_module");
    PyObject* py_imp_load_module = PyDict_GetItemString(py_imp_dict, "load_module");

    // Prepare arguments for find_module ( module name, [ directories ] )

    PyObject* py_mod_name = PyString_FromString("pySubmitter");
    PyObject* py_mod_dir = PyString_FromString(Arc::ArcLocation::GetDataDir().c_str());
    PyObject* py_dir_list = PyList_New(1);
    PyObject* py_find_args = PyTuple_New(2);

    PyList_SetItem(py_dir_list, 0, py_mod_dir); 
    PyTuple_SetItem(py_find_args, 0, py_mod_name); 
    PyTuple_SetItem(py_find_args, 1, py_dir_list); 

    PyObject* py_mod_info = PyObject_CallObject(py_imp_find_module, py_find_args); 
    // = ( open file object, path, description )
    if (!py_mod_info) return false;

    // Prepare arguments for load_module ( module name, ( find_module results ) )

    PyObject* py_load_args = PyTuple_New(4);
    PyTuple_SetItem(py_load_args, 0, py_mod_name);
    PyTuple_SetItem(py_load_args, 1, PyTuple_GetItem(py_mod_info, 0));
    PyTuple_SetItem(py_load_args, 2, PyTuple_GetItem(py_mod_info, 1));
    PyTuple_SetItem(py_load_args, 3, PyTuple_GetItem(py_mod_info, 2));
    py_mod = PyObject_CallObject(py_imp_load_module, py_load_args);
    if (!py_mod) return false;

    PyObject* py_mod_dict = PyModule_GetDict(py_mod);
    PyObject* py_submit_name = PyString_FromString("submit");
    PyObject* py_cancel_name = PyString_FromString("cancel");
    py_submit = PyDict_GetItem(py_mod_dict, py_submit_name);
    py_cacnel = PyDict_GetItem(py_mod_dict, py_cancel_name);
    Py_DECREF(py_submit_name);
    Py_DECREF(py_cancel_name);
    if (!py_submit || !py_cancel) return false;

    py_lrms = PyString_FromString(lrms.c_str());

    logger.msg(Arc::VERBOSE, "PythonPlugin initialized");
    return true;
  }

  std::string PythonPlugin::submit(Arc::JobDescription* j) {
    if (!py_submit) return false;
    logger.msg(Arc::VERBOSE, "Calling SUBMIT function");
    PyObject* py_job = PyCObject_FromVoidPtr((void*) j, NULL);
    PyObject* py_res = PyObject_CallFunctionObjArgs(py_submit, py_job, py_lrms, NULL);    
    Py_DECREF(py_job);
    Py_DECREF(py_job);
    if (py_res)
      return PyObject_AsString(py_res);
    return '\0';
  }

  bool PythonPlugin::cancel(const std::string& localid) {
    if (!py_cancel) return false;
    logger.msg(Arc::VERBOSE, "Calling CANCEL function");
    PyObject* py_localid = PyObject_FromString(localid.c_str());
    PyObject* py_res = PyObject_CallFunctionObjArgs(py_cancel, py_localid, py_lrms, NULL);    
    if (py_res)
      return PyObject_AsInt(py_res) == 0;
    return false;
  }

  PythonPlugin::~PythonPlugin(void) {
    if (py_submit) Py_DECREF(py_submit);
    if (py_cancel) Py_DECREF(py_cancel);
    if (py_mod) Py_DECREF(py_mod);
    if (py_lrms) Py_XDECREF(py_lrms);
  }
}
