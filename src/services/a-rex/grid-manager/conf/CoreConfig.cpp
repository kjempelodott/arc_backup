#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <iostream>
#include <pwd.h>

#include <arc/ArcConfig.h>
#include <arc/ArcLocation.h>
#include <arc/StringConv.h>
#include <arc/Utils.h>
#include <arc/XMLNode.h>
#include "../jobs/ContinuationPlugins.h"
#include "../run/RunPlugin.h"
#include "../misc/escaped.h"
#include "../log/JobLog.h"
#include "../jobs/JobsList.h"

#include "ConfigUtils.h"
#include "ConfigSections.h"
#include "CacheConfig.h"
#include "GMConfig.h"

#include "CoreConfig.h"

namespace ARex {

Arc::Logger CoreConfig::logger(Arc::Logger::getRootLogger(), "CoreConfig");
#define REPORTER_PERIOD "3600";

void CoreConfig::CheckLRMSBackends(const std::string& default_lrms, bool use_python_lrms) {
  if (use_python_lrms) {
    std::string tool_path = Arc::ArcLocation::GetDataDir() + "/pyCancel.py";
    if(!Glib::file_test(tool_path,Glib::FILE_TEST_IS_REGULAR)) {
      logger.msg(Arc::WARNING, "Missing pyCancel.py - job cancellation may not work");
    }
    tool_path = Arc::ArcLocation::GetDataDir() + "/pySubmit.py";
    if(!Glib::file_test(tool_path,Glib::FILE_TEST_IS_REGULAR)) {
      logger.msg(Arc::WARNING, "Missing pySubmit.py - job submission to LRMS may not work");
    }
    tool_path = Arc::ArcLocation::GetDataDir() + "/pyScanner.py";
    if(!Glib::file_test(tool_path,Glib::FILE_TEST_IS_REGULAR)) {
      logger.msg(Arc::WARNING, "Missing pyScanner.py - may miss when job finished executing");
    }
  }
  else {
    std::string tool_path;
    tool_path=Arc::ArcLocation::GetDataDir()+"/cancel-"+default_lrms+"-job";
    if(!Glib::file_test(tool_path,Glib::FILE_TEST_IS_REGULAR)) {
      logger.msg(Arc::WARNING,"Missing cancel-%s-job - job cancellation may not work",default_lrms);
    }
    tool_path=Arc::ArcLocation::GetDataDir()+"/submit-"+default_lrms+"-job";
    if(!Glib::file_test(tool_path,Glib::FILE_TEST_IS_REGULAR)) {
      logger.msg(Arc::WARNING,"Missing cancel-%s-job - job cancellation may not work",default_lrms);
    }
    tool_path=Arc::ArcLocation::GetDataDir()+"/scan-"+default_lrms+"-job";
    if(!Glib::file_test(tool_path,Glib::FILE_TEST_IS_REGULAR)) {
      logger.msg(Arc::WARNING,"Missing scan-%s-job - may miss when job finished executing",default_lrms);
    }
  }
}

bool CoreConfig::CheckYesNoCommand(bool& config_param, const std::string& name, std::string& rest) {
  std::string s = config_next_arg(rest);
  if (s == "yes") {
    config_param = true;
  }
  else if(s == "no") {
    config_param = false;
  }
  else {
    logger.msg(Arc::ERROR, "Wrong option in %s", name);
    return false;
  }
  return true;
}

bool CoreConfig::ParseConf(GMConfig& config) {

  if (config.xml_cfg) {
    return ParseConfXML(config, config.xml_cfg);
  }
  if (!config.conffile.empty()) {
    std::ifstream cfile;
    if (!config_open(cfile, config.conffile)) {
      logger.msg(Arc::ERROR, "Can't read configuration file at %s", config.conffile);
      return false;
    }
    // detect type of file
    config_file_type type = config_detect(cfile);
    if (type == config_file_XML) {
      Arc::XMLNode xml_cfg;
      if (!xml_cfg.ReadFromStream(cfile)) {
        config_close(cfile);
        logger.msg(Arc::ERROR, "Can't interpret configuration file %s as XML", config.conffile);
        return false;
      }
      config_close(cfile);
      // Pick out the A-REX service node
      Arc::XMLNode arex;
      Arc::Config cfg(xml_cfg);
      if (!cfg) return false;

      if (cfg.Name() == "Service") {
        if (cfg.Attribute("name") == "a-rex") {
          cfg.New(arex);
          return ParseConfXML(config, arex);
        }
        return false; // not a-rex service
      }
      if (cfg.Name() == "ArcConfig") {
        // In the case of multiple A-REX services defined, we parse the first one
        for (int i=0;; i++) {
          Arc::XMLNode node = cfg["Chain"];
          node = node["Service"][i];
          if (!node) return false; // no a-rex node found
          if (node.Attribute("name") == "a-rex") {
            node.New(arex);
            break;
          }
        }
        if (!arex) return false;
        return ParseConfXML(config, arex);
      }
      // malformed xml
      return false;
    }
    if (type == config_file_INI) {
      bool result = ParseConfINI(config, cfile);
      config_close(cfile);
      return result;
    }
    logger.msg(Arc::ERROR, "Can't recognize type of configuration file at %s", config.conffile);
    return false;
  }
  logger.msg(Arc::ERROR, "Could not determine configuration type or configuration is empty");
  return false;
}

bool CoreConfig::ParseConfINI(GMConfig& config, std::ifstream& cfile) {

  // List of helper commands that will be substituted after all configuration is read
  std::list<std::string> helpers;
  ConfigSections cf(cfile);
  cf.AddSection("common");
  cf.AddSection("grid-manager");
  cf.AddSection("infosys");
  cf.AddSection("queue");
  // process configuration information here
  for(;;) {
    std::string rest;
    std::string command;
    cf.ReadNext(command, rest);
    if (cf.SectionNum() == 2) { // infosys - looking for user name to get share uid
      if (command == "user") {
        config.SetShareID(Arc::User(rest));
      }
      continue;
    }
    if (cf.SectionNum() == 0) { // infosys user may be in common too
      if (command == "user") {
        config.SetShareID(Arc::User(rest));
      } else if(command == "x509_cert_dir") {
        config.cert_dir = rest;
      } else if(command == "x509_voms_dir") {
        config.voms_dir = rest;
      }
      // no continue since some options can be in common or grid-manager
    }
    if (cf.SectionNum() == 3) { // queue
      if (cf.SectionNew()) {
        std::string name = cf.SubSection();
        if (name.empty()) {
          logger.msg(Arc::ERROR, "No queue name given in queue block name"); return false;
        }
        config.queues.push_back(name);
      }
      continue;
    }
    if (command.empty()) { // EOF
      break;
    }
    if(command == "arex_mount_point") {
       config.arex_endpoint = config_next_arg(rest);
    }
    else if (command == "runtimedir") {
      config.rte_dir = rest;
    }
    else if (command == "joblog") { // where to write job information
      if (!config.job_log) continue;
      std::string fname = config_next_arg(rest);  // empty is allowed too
      config.job_log->SetOutput(fname.c_str());
    }
    else if (command == "jobreport") { // service to report information to
      if (!config.job_log) continue;
      for(;;) {
        std::string url = config_next_arg(rest);
        if (url.empty()) break;
        unsigned int i;
        if (Arc::stringto(url, i)) {
          config.job_log->SetExpiration(i);
          continue;
        }
        config.job_log->SetReporter(url.c_str());
      }
    }
    else if (command == "jobreport_vo_filters") { // which VO will be send to the server
      if (!config.job_log) continue;
      for(;;) {
        std::string voFilters = config_next_arg(rest);
        if (voFilters.empty()) break;
        config.job_log->SetVoFilters(voFilters.c_str());
      }
    }
    else if (command == "jobreport_publisher") { // Name of the publisher: e.g. jura
      if (!config.job_log) continue;
      std::string publisher = config_next_arg(rest);
      if (publisher.empty()) {
        publisher = "jura";
      }
      config.job_log->SetLogger(publisher.c_str());
    }
    else if (command == "jobreport_period") { // Period of running in seconds: e.g. 3600
      if (!config.job_log) continue;
      std::string period_s = config_next_arg(rest);
      if (period_s.empty()) {
        period_s = REPORTER_PERIOD;
      }
      int period;
      if (!Arc::stringto(period_s, period)) {
        logger.msg(Arc::ERROR, "Wrong number in jobreport_period: %s", period_s); return false;
      }
      if (!config.job_log->SetPeriod(period)) {
        std::string default_value = REPORTER_PERIOD;
        logger.msg(Arc::ERROR, "Wrong number in jobreport_period: %d, minimal value: %s", period, default_value); return false;
      }
    }
    else if (command == "jobreport_credentials") {
      if (!config.job_log) continue;
      std::string jobreport_key = config_next_arg(rest);
      std::string jobreport_cert = config_next_arg(rest);
      std::string jobreport_cadir = config_next_arg(rest);
      config.job_log->SetCredentials(jobreport_key, jobreport_cert, jobreport_cadir);
    }
    else if (command == "jobreport_options") { // e.g. for SGAS, interpreted by usage reporter
      if (!config.job_log) continue;
      std::string accounting_options = config_next_arg(rest);
      config.job_log->SetOptions(accounting_options);
    }
    else if (command == "jobreport_logfile") {
      if (!config.job_log) continue;
      std::string logfile = config_next_arg(rest);
      if (logfile.empty()) {
        logger.msg(Arc::ERROR, "Missing file name in jobreport_logfile"); return false;
      }
      config.job_log->SetLogFile(logfile.c_str());
    }
    else if (command == "scratchdir") {
      std::string scratch = config_next_arg(rest);
      // don't set if already set by shared_scratch
      if (config.scratch_dir.empty()) config.scratch_dir = scratch;
    }
    else if (command == "shared_scratch") {
      std::string scratch = config_next_arg(rest);
      config.scratch_dir = scratch;
    }
    else if (command == "maxjobs") { // maximum number of the jobs to support
      std::string max_jobs_s = config_next_arg(rest);
      if (max_jobs_s.empty()) continue;
      if (!Arc::stringto(max_jobs_s, config.max_jobs)) {
        logger.msg(Arc::ERROR, "Wrong number in maxjobs: %s", max_jobs_s); return false;
      }
      if (config.max_jobs < 0) config.max_jobs = -1;

      max_jobs_s = config_next_arg(rest);
      if (max_jobs_s.empty()) continue;
      if (!Arc::stringto(max_jobs_s, config.max_jobs_running)) {
        logger.msg(Arc::ERROR, "Wrong number in maxjobs: %s", max_jobs_s); return false;
      }
      if (config.max_jobs_running < 0) config.max_jobs_running = -1;

      max_jobs_s = config_next_arg(rest);
      if (max_jobs_s.empty()) continue;
      if (!Arc::stringto(max_jobs_s, config.max_jobs_per_dn)) {
        logger.msg(Arc::ERROR, "Wrong number in maxjobs: %s", max_jobs_s); return false;
      }
      if (config.max_jobs_per_dn < 0) config.max_jobs_per_dn = -1;

      max_jobs_s = config_next_arg(rest);
      if (max_jobs_s.empty()) continue;
      if (!Arc::stringto(max_jobs_s, config.max_jobs_total)) {
        logger.msg(Arc::ERROR, "Wrong number in maxjobs: %s", max_jobs_s); return false;
      }
      if (config.max_jobs_total < 0) config.max_jobs_total = -1;
    }
    else if (command == "wakeupperiod") {
      std::string wakeup_s = config_next_arg(rest);
      if (!Arc::stringto(wakeup_s, config.wakeup_period)) {
        logger.msg(Arc::ERROR,"Wrong number in wakeupperiod: %s",wakeup_s); return false;
      }
    }
    else if(command == "norootpower") {
      if (!CheckYesNoCommand(config.strict_session, command, rest)) return false;
    }
    else if (command == "mail") { // internal address from which to send mail
      config.support_email_address = config_next_arg(rest);
      if (config.support_email_address.empty()) {
        logger.msg(Arc::ERROR, "mail parameter is empty"); return false;
      }
    }
    else if (command == "defaultttl") { // time to keep job after finished
      std::string default_ttl_s = config_next_arg(rest);
      if (!Arc::stringto(default_ttl_s, config.keep_finished)) {
        logger.msg(Arc::ERROR, "Wrong number in defaultttl command"); return false;
      }
      default_ttl_s = config_next_arg(rest);
      if (!default_ttl_s.empty() && !Arc::stringto(default_ttl_s, config.keep_deleted)) {
        logger.msg(Arc::ERROR, "Wrong number in defaultttl command"); return false;
      }
    }
    else if (command == "maxrerun") { // number of retries allowed
      std::string default_reruns_s = config_next_arg(rest);
      if (!Arc::stringto(default_reruns_s, config.reruns)) {
        logger.msg(Arc::ERROR, "Wrong number in maxrerun command"); return false;
      }
    }
    else if (command == "lrms") { // default lrms type and queue (optional)
      std::string default_lrms = config_next_arg(rest);
      if (default_lrms.empty()) {
        logger.msg(Arc::ERROR, "defaultlrms is empty"); return false;
      }
      config.default_lrms = default_lrms;
      std::string default_queue = config_next_arg(rest);
      if (!default_queue.empty()) {
        config.default_queue = default_queue;
      }
    }
    else if (command == "authplugin") { // set plugin to be called on state changes
      if (!config.cont_plugins) continue;
      std::string state_name = config_next_arg(rest);
      if (state_name.empty()) {
        logger.msg(Arc::ERROR, "State name for plugin is missing"); return false;
      }
      std::string options_s = config_next_arg(rest);
      if (options_s.empty()) {
        logger.msg(Arc::ERROR, "Options for plugin are missing"); return false;
      }
      if (!config.cont_plugins->add(state_name.c_str(), options_s.c_str(), rest.c_str())) {
        logger.msg(Arc::ERROR, "Failed to register plugin for state %s", state_name); return false;
      }
    }
    else if (command == "localcred") {
      if (!config.cred_plugin) continue;
      std::string timeout_s = config_next_arg(rest);
      int timeout;
      if (!Arc::stringto(timeout_s, timeout)){
        logger.msg(Arc::ERROR, "Wrong number for timeout in plugin command"); return false;
      }
      config.cred_plugin->timeout(timeout);
    }
    else if (command == "fixdirectories") {
      std::string s = config_next_arg(rest);
      if (s == "yes") {
        config.fixdir = GMConfig::fixdir_always;
      }
      else if (s == "missing") {
        config.fixdir = GMConfig::fixdir_missing;
      }
      else if (s == "no") {
        config.fixdir = GMConfig::fixdir_never;
      }
      else {
        logger.msg(Arc::ERROR, "Wrong option in fixdirectories"); return false;
      }
    }
    else if (command == "allowsubmit") { // Note: not available in xml
      config.allow_submit += " " + config_next_arg(rest);
    }
    else if (command == "enable_arc_interface") {
      if (!CheckYesNoCommand(config.enable_arc_interface, command, rest)) return false;
    }
    else if (command == "enable_emies_interface") {
      if (!CheckYesNoCommand(config.enable_emies_interface, command, rest)) return false;
    }
    else if (command == "sessiondir") {  // set session root directory
      std::string session_root = config_next_arg(rest);
      if (session_root.empty()) {
        logger.msg(Arc::ERROR, "Session root directory is missing"); return false;
      }
      if (rest.length() != 0 && rest != "drain") {
        logger.msg(Arc::ERROR, "Junk in sessiondir command"); return false;
      }
      if (session_root == "*") {
        // special value which uses each user's home area
        session_root = "%H/.jobs";
      }
      config.session_roots.push_back(session_root);
      if (rest != "drain") config.session_roots_non_draining.push_back(session_root);
    }
    else if (command == "controldir") {
      std::string control_dir = config_next_arg(rest);
      if (control_dir.empty()) {
        logger.msg(Arc::ERROR, "Missing directory in control command"); return false;
      }
      config.control_dir = control_dir;
    }
    else if (command == "control") {
      logger.msg(Arc::WARNING, "'control' configuration option is no longer supported, please use 'controldir' instead");
    }
    else if (command == "helper") {
      std::string helper_user = config_next_arg(rest);
      if (helper_user.empty()) {
        logger.msg(Arc::ERROR, "User for helper program is missing"); return false;
      }
      if (helper_user != ".") {
        logger.msg(Arc::ERROR, "Only user '.' for helper program is supported"); return false;
      }
      if (rest.empty()) {
        logger.msg(Arc::ERROR, "Helper program is missing"); return false;
      }
      helpers.push_back(rest);
    }
    else if (command == "use_python_lrms") {
      if (!CheckYesNoCommand(config.use_python_lrms, command, rest)) return false;
    }    
    // SSH
    else if (command == "remote_host") {
      std::string remote_host = config_next_arg(rest);
      if (remote_host.empty()) {
	logger.msg(Arc::ERROR, "Missing directory in remote_host command"); return false;
      }
      config.ssh_config.remote_host = remote_host;
    }
    else if (command == "remote_user") {
      std::string remote_user = config_next_arg(rest);
      if (remote_user.empty()) {
	logger.msg(Arc::ERROR, "Missing directory in remote_user command"); return false;
      }
      config.ssh_config.remote_user = remote_user;
    }
    else if (command == "remote_sessiondir") {
      std::string remote_session_dir = config_next_arg(rest);
      if (remote_session_dir.empty()) {
	logger.msg(Arc::ERROR, "Missing directory in remote_sessiondir command"); return false;
      }
      config.ssh_config.remote_session_dir = remote_session_dir;
    }
    else if (command == "remote_runtimedir") {
      std::string remote_runtime_dir = config_next_arg(rest);
      if (remote_runtime_dir.empty()) {
        logger.msg(Arc::ERROR, "Missing directory in remote_runtimedir command"); return false;
      }
      config.ssh_config.remote_runtime_dir = remote_runtime_dir;
    }
    else if (command == "remote_cachedir") {
      std::string remote_cache_dir = config_next_arg(rest);
      if (remote_cache_dir.empty()) {
        logger.msg(Arc::ERROR, "Missing directory in remote_cachedir command"); return false;
      }
      config.ssh_config.remote_cache_dir = remote_cache_dir;
    }
    else if (command == "private_key") {
      std::string private_key = config_next_arg(rest);
      if (private_key.empty()) {
        logger.msg(Arc::ERROR, "Missing directory in private_key command"); return false;
      }
      config.ssh_config.private_key = private_key;
    }
  }
  CheckLRMSBackends(config.default_lrms, config.use_python_lrms);
  // End of parsing conf commands

  // Do substitution of control dir and helpers here now we have all the
  // configuration. These are special because they do not change per-user
  config.Substitute(config.control_dir);
  for (std::list<std::string>::iterator helper = helpers.begin(); helper != helpers.end(); ++helper) {
    config.Substitute(*helper);
    config.helpers.push_back(*helper);
  }

  // Add helper to poll for finished LRMS jobs
  if (!config.default_lrms.empty() && !config.control_dir.empty()) {
    std::string cmd = Arc::ArcLocation::GetDataDir();
    cmd += config.use_python_lrms ? "/pyScanner.py" : "/scan-" + config.default_lrms + "-job";
    cmd = Arc::escape_chars(cmd, " \\", '\\', false);
    if (!config.conffile.empty())
      cmd += config.use_python_lrms ? " " + config.conffile + " " + config.default_lrms : " --config " + config.conffile;
    cmd += " " + config.control_dir;
    config.helpers.push_back(cmd);
  }

  // Get cache parameters
  try {
    CacheConfig cache_config = CacheConfig(config);
    config.cache_params = cache_config;
  }
  catch (CacheConfigException& e) {
    logger.msg(Arc::ERROR, "Error with cache configuration: %s", e.what());
    return false;
  }

  return true;
}

bool CoreConfig::ParseConfXML(GMConfig& config, const Arc::XMLNode& cfg) {

  // Currently we have everything running inside same arched.
  // So we do not need any special treatment for infosys.
  // std::string infosys_user("");

  Arc::XMLNode tmp_node = cfg["endpoint"];
  if (tmp_node) config.headnode = (std::string)tmp_node;
  /*
  jobLogPath

  jobReport
    destination
    expiration
    type
    parameters
    KeyPath
    CertificatePath
    CACertificatesDir
    logfile
  */
  tmp_node = cfg["enableARCInterface"];
  if (tmp_node) {
    if (Arc::lower((std::string)tmp_node) == "yes") {
      config.enable_arc_interface = true;
    } else {
      config.enable_arc_interface = false;
    }
  }
  tmp_node = cfg["enableEMIESInterface"];
  if (tmp_node) {
    if (Arc::lower((std::string)tmp_node) == "yes") {
      config.enable_emies_interface = true;
    } else {
      config.enable_emies_interface = false;
    }
  }
  tmp_node = cfg["jobLogPath"];
  if (tmp_node && config.job_log) {
    std::string fname = tmp_node;
    config.job_log->SetOutput(fname.c_str());
  }
  tmp_node = cfg["jobReport"];
  if (tmp_node && config.job_log) {
    std::string url = tmp_node["destination"];
    if (!url.empty()) {
      // destination is required
      config.job_log->SetReporter(url.c_str());
      std::string publisher = tmp_node["publisher"];
      if (publisher.empty()) publisher = "jura";
      config.job_log->SetLogger(publisher.c_str());
      unsigned int i;
      if (Arc::stringto(tmp_node["expiration"], i)) config.job_log->SetExpiration(i);
      std::string period = tmp_node["Period"];
      if (period.empty()) period = REPORTER_PERIOD;
      unsigned int p;
      if (!Arc::stringto(period, p)) {
        logger.msg(Arc::ERROR, "Wrong number in jobreport_period: %s", period); return false;
      }
      if (!config.job_log->SetPeriod(p)) {
        std::string default_value = REPORTER_PERIOD;
        logger.msg(Arc::ERROR, "Wrong number in jobreport_period: %d, minimal value: %s", p, default_value); return false;
      }      
      std::string parameters = tmp_node["parameters"];
      if (!parameters.empty()) config.job_log->SetOptions(parameters);
      std::string jobreport_key = tmp_node["KeyPath"];
      std::string jobreport_cert = tmp_node["CertificatePath"];
      std::string jobreport_cadir = tmp_node["CACertificatesDir"];
      config.job_log->SetCredentials(jobreport_key, jobreport_cert, jobreport_cadir);
      std::string logfile = tmp_node["logfile"];
      if (!logfile.empty()) config.job_log->SetLogFile(logfile.c_str());
    }
  }

  /*
  loadLimits
    maxJobsTracked
    maxJobsRun
    maxJobsTotal
    maxJobsPerDN
    wakeupPeriod
  */
  tmp_node = cfg["loadLimits"];
  if (tmp_node) {
    if (!elementtoint(tmp_node, "maxJobsTracked", config.max_jobs, &logger)) return false;
    if (!elementtoint(tmp_node, "maxJobsRun", config.max_jobs_running, &logger)) return false;
    if (!elementtoint(tmp_node, "maxJobsTotal", config.max_jobs_total, &logger)) return false;
    if (!elementtoint(tmp_node, "maxJobsPerDN", config.max_jobs_per_dn, &logger)) return false;
    if (!elementtoint(tmp_node, "wakeupPeriod", config.wakeup_period, &logger)) return false;
  }

  /*
  serviceMail
  */
  tmp_node = cfg["serviceMail"];
  if(tmp_node) {
    config.support_email_address = (std::string)tmp_node;
    if (config.support_email_address.empty()) {
      logger.msg(Arc::ERROR, "serviceMail is empty");
      return false;
    }
  }

  /*
  LRMS
    type
    defaultShare
    usePythonLRMS
  */
  tmp_node = cfg["LRMS"];
  if (tmp_node) {
    config.default_lrms = (std::string)(tmp_node["type"]);
    if(config.default_lrms.empty()) {
      logger.msg(Arc::ERROR,"Type in LRMS is missing");
      return false;
    }
    config.default_queue = (std::string)(tmp_node["defaultShare"]);
    config.use_python_lrms = (Arc::lower((std::string)tmp_node["usePythonLRMS"]) == "yes");
    CheckLRMSBackends(config.default_lrms, config.use_python_lrms);
    config.rte_dir = (std::string)(tmp_node["runtimeDir"]);
    // We only want the scratch path as seen on the front-end
    if (tmp_node["sharedScratch"]) {
      config.scratch_dir = (std::string)(tmp_node["sharedScratch"]);
    } else if (tmp_node["scratchDir"]) {
      config.scratch_dir = (std::string)(tmp_node["scratchDir"]);
    }
  } else {
    logger.msg(Arc::ERROR, "LRMS is missing");
    return false;
  }

  /*
  authPlugin (timeout,onSuccess=PASS,FAIL,LOG,onFailure=FAIL,PASS,LOG,onTimeout=FAIL,PASS,LOG)
    state
    command
  */
  tmp_node = cfg["authPlugin"];
  if (config.cont_plugins) for (; tmp_node; ++tmp_node) {
    std::string state_name = tmp_node["state"];
    if (state_name.empty()) {
      logger.msg(Arc::ERROR, "State name for authPlugin is missing");
      return false;
    }
    std::string command = tmp_node["command"];
    if (state_name.empty()) {
      logger.msg(Arc::ERROR, "Command for authPlugin is missing");
      return false;
    }
    std::string options;
    Arc::XMLNode onode;
    onode = tmp_node.Attribute("timeout");
    if (onode) options += "timeout="+(std::string)onode+',';
    onode = tmp_node.Attribute("onSuccess");
    if (onode) options += "onsuccess="+Arc::lower((std::string)onode)+',';
    onode = tmp_node.Attribute("onFailure");
    if (onode) options += "onfailure="+Arc::lower((std::string)onode)+',';
    onode = tmp_node.Attribute("onTimeout");
    if (onode) options += "ontimeout="+Arc::lower((std::string)onode)+',';
    if (!options.empty()) options = options.substr(0, options.length()-1);
    logger.msg(Arc::DEBUG, "Registering plugin for state %s; options: %s; command: %s",
        state_name, options, command);
    if (!config.cont_plugins->add(state_name.c_str(), options.c_str(), command.c_str())) {
      logger.msg(Arc::ERROR, "Failed to register plugin for state %s", state_name);
      return false;
    }
  }

  /*
  localCred (timeout)
    command
  */
  tmp_node = cfg["localCred"];
  if (tmp_node && config.cred_plugin) {
    std::string command = tmp_node["command"];
    if (command.empty()) {
      logger.msg(Arc::ERROR, "Command for localCred is missing");
      return false;
    }
    Arc::XMLNode onode;
    onode = tmp_node.Attribute("timeout");
    if (!onode) {
      logger.msg(Arc::ERROR, "Timeout for localCred is missing");
      return false;
    }
    int to;
    if (!elementtoint(onode, NULL, to, &logger)) {
      logger.msg(Arc::ERROR, "Timeout for localCred is incorrect number");
      return false;
    }
    *(config.cred_plugin) = command;
    config.cred_plugin->timeout(to);
  }

  /*
  control
    username <- not used any more
    controlDir
    sessionRootDir
    ssh
      remoteHost
      remoteUser
      remoteSessionRootDir
      remoteRuntimeDir
      remoteCacheDir
      privateKey  
    cache
      location
        path
        link
      highWatermark
      lowWatermark
    defaultTTL
    defaultTTR
    maxReruns
    noRootPower
    fixDirectories
    diskSpace <- not used any more
  */

  tmp_node = cfg["control"];
  if (!tmp_node) {
    logger.msg (Arc::ERROR, "Control element must be present");
    return false;
  }
  config.control_dir = (std::string)(tmp_node["controlDir"]);
  if (config.control_dir.empty()) {
    logger.msg(Arc::ERROR, "controlDir is missing");
    return false;
  }
  Arc::XMLNode session_node = tmp_node["sessionRootDir"];
  for (;session_node; ++session_node) {
    std::string session_root = std::string(session_node);
    if (session_root.empty()) {
      logger.msg(Arc::ERROR,"sessionRootDir is missing");
      return false;
    }
    if (session_root == "*") {
      // special value which uses each user's home area
      session_root = "%H/.jobs";
    }
    config.session_roots.push_back(session_root);
    bool session_drain = false;
    if(!elementtobool(session_node.Attribute("drain"), NULL, session_drain, &logger)) return false;
    if(!session_drain) config.session_roots_non_draining.push_back(session_root);
  }
  GMConfig::fixdir_t fixdir = GMConfig::fixdir_always;
  const char* fixdir_opts[] = { "yes", "missing", "no", NULL };
  int n;
  if (!elementtoenum(tmp_node, "fixDirectories", n=(int)fixdir, fixdir_opts, &logger)) return false;
  config.fixdir = (GMConfig::fixdir_t)n;
  if (!elementtoint(tmp_node, "maxReruns", config.reruns, &logger)) return false;
  if (!elementtobool(tmp_node, "noRootPower", config.strict_session, &logger)) return false;
  if (!elementtoint(tmp_node, "defaultTTL", config.keep_finished, &logger)) return false;
  if (!elementtoint(tmp_node, "defaultTTR", config.keep_deleted, &logger)) return false;

  // Get cache parameters
  try {
    CacheConfig cache_config(tmp_node);
    config.cache_params = cache_config;
  }
  catch (CacheConfigException& e) {
    logger.msg(Arc::ERROR, "Error with cache configuration: %s", e.what());
    return false;
  }

  // Get ssh parameters
  Arc::XMLNode to_node  = tmp_node["ssh"];
  if (to_node) {
    config.ssh_config.remote_host = (std::string)to_node["remoteHost"];
    if (config.ssh_config.remote_host.empty()) {
      logger.msg(Arc::ERROR, "remoteHost is missing");
      return false;
    }
    config.ssh_config.remote_user = (std::string)to_node["remoteUser"];
    if (config.ssh_config.remote_user.empty()) {
      logger.msg(Arc::ERROR, "remoteUser is missing");
      return false;
    }
    config.ssh_config.remote_session_dir = (std::string)to_node["remoteSessionRootDir"];
    if (config.ssh_config.remote_session_dir.empty()) {
      logger.msg(Arc::ERROR, "remoteSessionRootDir is missing");
      return false;
    }
    config.ssh_config.private_key = (std::string)to_node["privateKey"];
    if (config.ssh_config.private_key.empty()) {
      logger.msg(Arc::ERROR, "privateKey is missing");
      return false;
    }
    config.ssh_config.remote_runtime_dir = (std::string)to_node["remoteRuntimeDir"];
    config.ssh_config.remote_cache_dir = (std::string)to_node["remoteCacheDir"];
  }

  /*
  helperUtility
    username
    command
  */
  std::list<std::string> helpers;
  tmp_node = cfg["helperUtility"];
  for(; tmp_node; ++tmp_node) {
    std::string command = tmp_node["command"];
    if (command.empty()) {
      logger.msg(Arc::ERROR, "Command in helperUtility is missing");
      return false;
    }
    std::string username = tmp_node["username"];
    if (username.empty()) {
      logger.msg(Arc::ERROR, "Username in helperUtility is empty");
      return false;
    }
    if (username != ".") {
      logger.msg(Arc::ERROR, "Only user '.' for helper program is supported");
      return false;
    }
    helpers.push_back(command);
  }
  // End of parsing XML node

  // Do substitution of control dir and helpers here now we have all the
  // configuration. These are special because they do not change per-user
  config.Substitute(config.control_dir);
  for (std::list<std::string>::iterator helper = helpers.begin(); helper != helpers.end(); ++helper) {
    config.Substitute(*helper);
    config.helpers.push_back(*helper);
  }

  // Add helper to poll for finished LRMS jobs
  std::string cmd = Arc::ArcLocation::GetDataDir();
  cmd += config.use_python_lrms ? "/pyScanner.py" : "/scan-" + config.default_lrms + "-job";
  cmd = Arc::escape_chars(cmd, " \\", '\\', false);
  if (!config.conffile.empty())
    cmd += config.use_python_lrms ? " " + config.conffile + " " + config.default_lrms : " --config " + config.conffile;
  cmd += " " + config.control_dir;
  config.helpers.push_back(cmd);

  return true;
}

} // namespace ARex
