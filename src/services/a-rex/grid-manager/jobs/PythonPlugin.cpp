#include "Python.h"

#include <sstream>

#include "PythonPlugin.h"
#include "arc/ArcLocation.h"


// 'std::to_string not a member of std' in old gcc versions
std::string int_to_string(int val)
{
  std::stringstream stream;
  stream << val;
  return stream.str();
}

namespace ARex {

  Arc::Logger PythonPlugin::logger(Arc::Logger::getRootLogger(), "PythonPlugin");

  // TODO: keep python arc.conf in memory, instead of reading pickle file every time
  PythonPlugin::PythonPlugin(void) : py_submit(NULL), py_cancel(NULL), 
				     py_lrms(NULL), py_conf(NULL) {}

  bool PythonPlugin::init(const std::string& lrms, const std::string& conf) {

    Py_Initialize();

    // Import the 'imp' library with its find_module and load_module functions
    PyObject* py_imp_str = PyString_FromString("imp");
    PyObject* py_imp_handle = PyImport_Import(py_imp_str);
    PyObject* py_imp_dict = PyModule_GetDict(py_imp_handle);
    PyObject* py_imp_find_module = PyDict_GetItemString(py_imp_dict, "find_module");

    // Prepare arguments for find_module ( module name, [ directories ] )
    PyObject* py_mod_name = PyString_FromString("pyModule");
    PyObject* py_mod_dir = PyString_FromString(Arc::ArcLocation::GetDataDir().c_str());
    PyObject* py_dir_list = PyList_New(1);
    PyObject* py_find_args = PyTuple_New(2);

    PyList_SetItem(py_dir_list, 0, py_mod_dir); 
    PyTuple_SetItem(py_find_args, 0, py_mod_name);
    PyTuple_SetItem(py_find_args, 1, py_dir_list); 
    PyObject* py_mod_info = PyObject_CallObject(py_imp_find_module, py_find_args); 
    
    Py_DECREF(py_find_args);
    Py_DECREF(py_imp_find_module);
    Py_DECREF(py_imp_handle);
    Py_DECREF(py_imp_str);

    // = ( open file object, path, description )
    if (!py_mod_info) return false;

    // Prepare arguments for load_module ( module name, ( find_module results ) )
    PyObject* mod_info_0 = PyTuple_GetItem(py_mod_info, 0);
    PyObject* mod_info_1 = PyTuple_GetItem(py_mod_info, 1);
    PyObject* mod_info_2 = PyTuple_GetItem(py_mod_info, 2);
    Py_INCREF(mod_info_0);
    Py_INCREF(mod_info_1);
    Py_INCREF(mod_info_2);

    PyObject* py_load_args = PyTuple_New(4);
    PyTuple_SetItem(py_load_args, 0, py_mod_name);
    PyTuple_SetItem(py_load_args, 1, mod_info_0);
    PyTuple_SetItem(py_load_args, 2, mod_info_1);
    PyTuple_SetItem(py_load_args, 3, mod_info_2);

    PyObject* py_imp_load_module = PyDict_GetItemString(py_imp_dict, "load_module");
    PyObject* py_mod = PyObject_CallObject(py_imp_load_module, py_load_args);

    Py_DECREF(py_load_args);
    Py_DECREF(py_imp_load_module);
    Py_DECREF(py_mod_info);

    if (!py_mod) return false;

    // Get submit and cancel functions from pyModule
    PyObject* py_mod_dict = PyModule_GetDict(py_mod);
    PyObject* py_submit_name = PyString_FromString("submit");
    PyObject* py_cancel_name = PyString_FromString("cancel");
    // Modules live forever, so no need to INCREF these
    py_submit = PyDict_GetItem(py_mod_dict, py_submit_name);
    py_cancel = PyDict_GetItem(py_mod_dict, py_cancel_name);

    Py_DECREF(py_submit_name);
    Py_DECREF(py_cancel_name);
    Py_DECREF(py_mod);

    if (!py_submit || !py_cancel) return false;

    py_lrms = PyString_FromString(lrms.c_str());
    py_conf = PyString_FromString(conf.c_str());

    logger.msg(Arc::INFO, "PythonPlugin initialized");
    /*
    logger.msg(Arc::INFO, "py_imp_str ref count: %d", py_imp_str->ob_refcnt);
    logger.msg(Arc::INFO, "py_imp_handle ref count: %d", py_imp_handle->ob_refcnt);
    logger.msg(Arc::INFO, "py_imp_dict ref count: %d", py_imp_dict->ob_refcnt);
    logger.msg(Arc::INFO, "py_imp_find_module ref count: %d", py_imp_find_module->ob_refcnt);
    logger.msg(Arc::INFO, "py_imp_load_module ref count: %d", py_imp_load_module->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_name ref count: %d", py_mod_name->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_dir ref count: %d", py_mod_dir->ob_refcnt);
    logger.msg(Arc::INFO, "py_dir_list ref count: %d", py_dir_list->ob_refcnt);
    logger.msg(Arc::INFO, "py_find_args ref count: %d", py_find_args->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_info ref count: %d", py_mod_info->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_info_0 ref count: %d", PyTuple_GetItem(py_mod_info, 0)->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_info_1 ref count: %d", PyTuple_GetItem(py_mod_info, 1)->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_info_2 ref count: %d", PyTuple_GetItem(py_mod_info, 2)->ob_refcnt);
    logger.msg(Arc::INFO, "py_load_args ref count: %d", py_load_args->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod ref count: %d", py_mod->ob_refcnt);
    logger.msg(Arc::INFO, "py_mod_dict ref count: %d", py_mod_dict->ob_refcnt);
    logger.msg(Arc::INFO, "py_submit_name ref count: %d", py_submit_name->ob_refcnt);
    logger.msg(Arc::INFO, "py_cancel_name ref count: %d", py_cancel_name->ob_refcnt);
    logger.msg(Arc::INFO, "py_submit ref count: %d", py_submit->ob_refcnt);
    logger.msg(Arc::INFO, "py_cancel ref count: %d", py_cancel->ob_refcnt);
    logger.msg(Arc::INFO, "py_lrms ref count: %d", py_lrms->ob_refcnt);
    logger.msg(Arc::INFO, "py_conf ref count: %d", py_conf->ob_refcnt);
    */
    return true;
  }

  std::string PythonPlugin::submit(Arc::JobDescription* j) {
    if (!py_submit) return "";
    logger.msg(Arc::INFO, "Calling SUBMIT function");
    PyObject* py_job = PyCObject_FromVoidPtr((void*) j, NULL);
    PyObject* py_res = PyObject_CallFunctionObjArgs(py_submit, py_conf, py_job, py_lrms, NULL);
    Py_DECREF(py_job);
    std::string retstr = "";
    if (py_res) {
      if (PyInt_Check(py_res))
	retstr = int_to_string(PyInt_AsLong(py_res));
      if (PyString_Check(py_res))
	logger.msg(Arc::ERROR, "Submit failed: %s", PyString_AsString(py_res));
      Py_DECREF(py_res);
    }
    return retstr;
  }

  bool PythonPlugin::cancel(const std::string& localid) {
    if (!py_cancel) return false;
    logger.msg(Arc::INFO, "Calling CANCEL function");
    PyObject* py_localid = PyString_FromString(localid.c_str());
    PyObject* py_res = PyObject_CallFunctionObjArgs(py_cancel, py_conf, py_localid, py_lrms, NULL);
    bool retval = false;
    if (py_res) {
      if (PyBool_Check(py_res))
	retval = py_res == Py_True;
      if (PyString_Check(py_res))
	logger.msg(Arc::ERROR, "Cancel failed: %s", PyString_AsString(py_res));
      Py_DECREF(py_res);
    }
    return retval;
  }

  PythonPlugin::~PythonPlugin(void) {
    if (py_submit) Py_DECREF(py_submit);
    if (py_cancel) Py_DECREF(py_cancel);
    if (py_lrms) Py_DECREF(py_lrms);
    if (py_conf) Py_DECREF(py_conf);
  }
}
