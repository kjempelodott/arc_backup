#include "PythonPlugin.h"
#include "arc/ArcLocation.h"

namespace ARex {

  Arc::Logger PythonPlugin::logger(Arc::Logger::getRootLogger(), "PythonPlugin");

  PythonPlugin::PythonPlugin(void) : py_func(NULL), py_mod(NULL) {}

  bool PythonPlugin::init(std::string module, std::string function) {

    Py_Initialize();

    PyObject *py_imp_str;
    PyObject *py_imp_handle;
    PyObject *py_imp_dict;
    PyObject *py_imp_find_module;
    PyObject *py_imp_load_module;

    PyObject *py_mod_name;
    PyObject *py_mod_dir;
    PyObject *py_dir_list;
    PyObject *py_find_args;
    PyObject *py_mod_info;    

    // Import the 'imp' library with its find_module and load_module functions
    py_imp_str = PyString_FromString("imp");
    py_imp_handle = PyImport_Import(py_imp_str);
    py_imp_dict = PyModule_GetDict(py_imp_handle);
    py_imp_find_module = PyDict_GetItemString(py_imp_dict, "find_module");
    py_imp_load_module = PyDict_GetItemString(py_imp_dict, "load_module");

    // Prepare arguments for find_module ( module name, [ directories ] )
    py_mod_name = PyString_FromString(module.c_str());
    py_mod_dir = PyString_FromString(Arc::ArcLocation::GetDataDir().c_str());
  
    py_dir_list = PyList_New(1);
    PyList_SetItem(py_dir_list, 0, py_mod_dir); // Takes ownership of py_mod_dir
    
    py_find_args = PyTuple_New(2);
    PyTuple_SetItem(py_find_args, 0, py_mod_name); // Takes ownership of py_mod_name
    PyTuple_SetItem(py_find_args, 1, py_dir_list);

    // find_module returns a tuple ( open file object, path, description )
    py_mod_info = PyObject_CallObject(py_imp_find_module, py_find_args);

    Py_DECREF(py_imp_str);
    if (!py_mod_info) return false;

    // ###

    PyObject *py_load_args;
    PyObject *py_mod_dict;
    PyObject *py_func_name;

    // Prepare arguments for load_module ( module name, ( find_module results ) )
    py_load_args = PyTuple_New(4);
    PyTuple_SetItem(py_load_args, 0, py_mod_name);
    PyTuple_SetItem(py_load_args, 1, PyTuple_GetItem(py_mod_info, 0));
    PyTuple_SetItem(py_load_args, 2, PyTuple_GetItem(py_mod_info, 1));
    PyTuple_SetItem(py_load_args, 3, PyTuple_GetItem(py_mod_info, 2));

    // Import module [py_mod] and  its function [py_func]
    py_mod = PyObject_CallObject(py_imp_load_module, py_load_args);

    if (!py_mod) return false;

    py_mod_dict = PyModule_GetDict(py_mod);
    py_func_name = PyString_FromString(function.c_str());
    py_func = PyDict_GetItem(py_mod_dict, py_func_name);

    Py_DECREF(py_func_name);
    if (!py_func) return false;

    return true;
  }

  void PythonPlugin::call(void *arg) {
    logger.msg(Arc::INFO, "calling python func");
    PyObject* py_arg = PyCObject_FromVoidPtr(arg, NULL);
    logger.msg(Arc::INFO, "made pyc obj");
    logger.msg(Arc::INFO, "mod ref: %p", py_mod);
    logger.msg(Arc::INFO, "func ref: %p", py_func);
    PyObject* py_res = PyObject_CallFunctionObjArgs(py_func, py_arg, NULL);
    logger.msg(Arc::INFO, "result ref: %p", py_res);
    Py_DECREF(py_arg);
  }

  PythonPlugin::~PythonPlugin(void) {
    if (py_func) Py_DECREF(py_func);
    if (py_mod) Py_DECREF(py_mod);
  }
}
