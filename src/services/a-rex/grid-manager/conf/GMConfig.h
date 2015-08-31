#ifndef GMCONFIG_H_
#define GMCONFIG_H_

#include <string>

#include <arc/Logger.h>
#include <arc/Run.h>
#include <arc/User.h>
#include <arc/XMLNode.h>

#include "CacheConfig.h"

namespace ARex {

// Forward declarations for classes for which this is just a container
class JobLog;
class ContinuationPlugins;
class RunPlugin;
class DelegationStores;

/// Configuration information related to the grid manager part of A-REX.
/**
 * This class contains all configuration variables related to grid-manager. It
 * also acts as a container for objects which are used in different parts of
 * A-REX. Therefore since this class contains pointers to complex objects, it
 * cannot be copied and hence the copy constructor and assignment operator are
 * private. Those pointers should be managed outside this class. GMConfig
 * should be instantiated once when the grid-manager is initialised and only
 * destroyed with the GM has finished. Ideally this would be a singleton but
 * that would prevent running multiple A-REXes in the same container.
 *
 * Substitutions are not done while parsing the configuration, as
 * substitution variables can change depending on the job. Therefore paths
 * are stored in their raw format, unsubstituted. The exception is the
 * control directory which cannot change and is substituted during parsing,
 * and helper options. Substitution of other variables should be done as
 * necessary using Substitute().
 */

/// SSH
typedef struct {
  std::string remote_session_dir;
  std::string remote_runtime_dir;
  std::string remote_cache_dir;
  std::string remote_user;
  std::string remote_host;
  std::string private_key;
  bool validated;
  bool ok;
} SSHConfig;

class GMConfig {

  // Main job loop which heavily uses this class
  friend class JobsList;
  // Configuration parser which sets values for members of this class
  friend class CoreConfig;
  // Parser of data-staging configuration which uses this class' values as default
  friend class StagingConfig;

public:

  /// Different options for fixing directories
  enum fixdir_t {
   fixdir_always,
   fixdir_missing,
   fixdir_never
  };

  /// Use given (or guessed if not given) configuration file.
  /**
   * Guessing uses $ARC_CONFIG, $ARC_LOCATION/etc/arc.conf or the default
   * location /etc/arc.conf. Load() should then be used to parse the
   * configuration and fill member variables.
   * @param conffile Path to configuration file, will be guessed if empty
   */
  GMConfig(const std::string& conffile="");
  /// Use the given XMLNode as a source of information instead of a file.
  GMConfig(const Arc::XMLNode& node);

  /// Load configuration from file or XML node into members of this object.
  /// Returns false if errors are found during parsing.
  bool Load();
  /// Print a summary of configuration to stderr
  void Print() const;

  /// Get path to configuration file
  const std::string& ConfigFile() const { return conffile; }
  /// Set path to configuration file
  void SetConfigFile(const std::string& file) { conffile = file; }
  /// Returns true if configuration file is temporary
  bool ConfigIsTemp() const { return conffile_is_temp; }
  /// Sets whether configuration file is temporary
  void SetConfigIsTemp(bool temp) { conffile_is_temp = temp; }
  /// Set XML node with configuration (corresponding to <Service name="a-rex">)
  void SetXMLNode(const Arc::XMLNode& node) { xml_cfg = node; }

  /// Create control structure with permissions depending on fixdir_t value.
  /// Typically called at A-REX service creation.
  bool CreateControlDirectory() const;
  /// Create session directory with correct permissions. Typically called when
  /// a new job is created and after all substitutions have been done. Creates
  /// session root if it does not already exist.
  bool CreateSessionDirectory(const std::string& dir, const Arc::User& user) const;

  /// Start/restart all helper processes
  bool RunHelpers();

  /// Substitute characters in param specified by % with real values. An
  /// optional User can be specified for the user-related substitutions.
  bool Substitute(std::string& param, const Arc::User& user=Arc::User()) const;

  /// Send signals to helpers to shut them down cleanly
  void PrepareToDestroy();

  /// Set control directory
  void SetControlDir(const std::string &dir);
  /// Set session root dir
  void SetSessionRoot(const std::string &dir);
  /// Set multiple session root dirs
  void SetSessionRoot(const std::vector<std::string> &dirs);
  /// Set uid and gids used by other process sharing information with A-REX
  void SetShareID(const Arc::User& share_user);
  /// Set default queue
  void SetDefaultQueue(const std::string& queue) { default_queue = queue; }

  /// Certificates directory location
  const std::string& CertDir() const { return cert_dir; }
  /// VOMS lsc files root directory location
  const std::string& VomsDir() const { return voms_dir; }
  /// Location of RTE setup scripts
  const std::string& RTEDir() const { return rte_dir; }
  /// Directory storing delegations
  std::string DelegationDir() const;

  /// email address of person responsible for this ARC installation
  const std::string& SupportMailAddress() const { return support_email_address; }

  /// Set JobLog object
  void SetJobLog(JobLog* log) { job_log = log; }
  /// Set ContinuationPlugins (plugins run at state transitions)
  void SetContPlugins(ContinuationPlugins* plugins) { cont_plugins = plugins; }
  /// Set RunPlugin (plugin used to acquire local credentials)
  void SetCredPlugin(RunPlugin* plugin) { cred_plugin = plugin; }
  /// Set DelegationStores object
  void SetDelegations(ARex::DelegationStores* stores) { delegations = stores; }
  /// JobLog object
  JobLog* GetJobLog() const { return job_log; }
  /// Plugins run at state transitions
  ContinuationPlugins* ContPlugins() const { return cont_plugins; }
  /// Plugin used to acquire local credentials
  RunPlugin* CredPlugin() const { return cred_plugin; }
  /// DelegationsStores object
  ARex::DelegationStores* Delegations() const { return delegations; }

  /// Control directory
  const std::string & ControlDir() const { return control_dir; }
  /// Session root directory corresponding to given job ID. If the session
  /// dir corresponding to job_id is not found an empty string is returned.
  std::string SessionRoot(const std::string& job_id) const;
  /// Session directories
  const std::vector<std::string> & SessionRoots() const { return session_roots; }
  /// Session directories that can be used for new jobs
  const std::vector<std::string> & SessionRootsNonDraining() const { return session_roots_non_draining; }
  /// Base scratch directory for job execution on node
  const std::string & ScratchDir() const { return scratch_dir; }
  /// Whether access to session dir must be performed under mapped uid
  bool StrictSession() const { return strict_session; }

  /// Cache configuration
  const CacheConfig & CacheParams() const { return cache_params; }

  /// URL of cluster's headnode
  const std::string & HeadNode() const { return headnode; }
  /// Whether ARC (BES) WS-interface is enabled
  bool ARCInterfaceEnabled() const { return enable_arc_interface; }
  /// Whether EMI-ES interface is enabled
  bool EMIESInterfaceEnabled() const { return enable_emies_interface; }
  /// GridFTP job interface endpoint
  const std::string & GridFTPEndpoint() const { return gridftp_endpoint; }
  /// A-REX WS-interface job submission endpoint
  const std::string & AREXEndpoint() const { return arex_endpoint; }

  /// Default LRMS
  const std::string & DefaultLRMS() const { return default_lrms; }
  /// Default queue
  const std::string & DefaultQueue() const { return default_queue; }
  /// All configured queues
  const std::list<std::string> & Queues() const { return queues; }

  /// Username of user running A-REX
  const std::string & UnixName() const { return gm_user.Name(); }

  /// Groups allowed to submit when general job submission is disabled
  const std::string & AllowSubmit() const { return allow_submit; }

  /// Length of time to keep session dir after job finishes
  time_t KeepFinished() const { return keep_finished; }
  /// Length of time to keep control information after job finishes
  time_t KeepDeleted() const { return keep_deleted; }

  /// Maximum number of job re-runs allowed
  int Reruns() const { return reruns; }

  /// Strategy for fixing directories
  fixdir_t FixDirectories() const { return fixdir; }

  /// Maxmimum time for A-REX to wait between job processing loops
  unsigned int WakeupPeriod() const { return wakeup_period; }

  /// Max jobs being processed (from PREPARING to FINISHING)
  int MaxJobs() const { return max_jobs; };
  /// Max jobs in the LRMS
  int MaxRunning() const { return max_jobs_running; }
  /// Max jobs being processed per-DN
  int MaxPerDN() const { return max_jobs_per_dn; }
  /// Max total jobs in the system
  int MaxTotal() const { return max_jobs_total; }

  /// Returns true if the shared uid matches the given uid
  bool MatchShareUid(uid_t suid) const { return ((share_uid==0) || (share_uid==suid)); };
  /// Returns true if any of the shared gids matches the given gid
  bool MatchShareGid(gid_t sgid) const;

  const SSHConfig& GetSSHConfig() const { return ssh_config; }
  bool UseSSH() const { return ssh_config.ok; }
  /// Check if remote directory is mounted
  bool SSHFS_OK(const std::string& mount_point) const;

private:

  /// Class to run external processes (helper)
  class ExternalHelper {
   private:
    /// Command being run
    std::string command;
    /// Object representing running process
    Arc::Run *proc;
   public:
    ExternalHelper(const std::string &cmd);
    ~ExternalHelper();
    /// Start process if it is not running yet
    bool run(const GMConfig& config);
    /// Stop process if it is running
    void stop();
  };
  /// Configuration file
  std::string conffile;
  /// Whether configuration file is temporary
  bool conffile_is_temp;
  /// Configuration passed as an XMLNode
  Arc::XMLNode xml_cfg;
  /// For logging job information to external logging service
  JobLog* job_log;
  /// Plugins run at certain state changes
  ContinuationPlugins* cont_plugins;
  /// Plugin for acquiring local credentials
  RunPlugin* cred_plugin;
  /// Delegated credentials stored by A-REX
  // TODO: this should go away after proper locking in DelegationStore is implemented
  ARex::DelegationStores* delegations;

  /// Certificates directory
  std::string cert_dir;
  /// VOMS LSC files directory
  std::string voms_dir;
  /// RTE directory
  std::string rte_dir;
  /// email address for support
  std::string support_email_address;

  /// Scratch directory
  std::string scratch_dir;
  /// Directory where files explaining jobs are stored
  std::string control_dir;
  /// Directories where directories used to run jobs are created
  std::vector<std::string> session_roots;
  /// Session directories allowed for new jobs (i.e. not draining)
  std::vector<std::string> session_roots_non_draining;
  /// Cache information
  CacheConfig cache_params;
  /// URL of the cluster's headnode
  std::string headnode;
  /// Default LRMS and queue to use
  std::string default_lrms;
  std::string default_queue;
  /// All configured queues
  std::list<std::string> queues;
  /// User running A-REX
  Arc::User gm_user;
  /// uid and gid(s) running other ARC processes that share files with A-REX
  uid_t share_uid;
  std::list<gid_t> share_gids;
  /// How long jobs are kept after job finished
  time_t keep_finished;
  time_t keep_deleted;
  /// Whether session must always be accessed under mapped user's uid
  bool strict_session;
  /// Strategy for fixing directories
  fixdir_t fixdir;
  /// Maximal value of times job is allowed to be rerun
  int reruns;
  /// Maximum time for A-REX to wait between each loop processing jobs
  unsigned int wakeup_period;
  /// Groups allowed to submit while job submission is disabled
  std::string allow_submit;
  /// List of associated external processes
  std::list<ExternalHelper> helpers;

  /// Maximum number of jobs running (between PREPARING and FINISHING)
  int max_jobs_running;
  /// Maximum total jobs in the system, including FINISHED and DELETED
  int max_jobs_total;
  /// Maximum jobs in the LRMS
  int max_jobs;
  /// Maximum jobs running per DN
  int max_jobs_per_dn;

  /// Whether WS-interface is enabled
  bool enable_arc_interface;
  /// Whether EMI-ES interface is enabled
  bool enable_emies_interface;
  /// GridFTP job endpoint
  std::string gridftp_endpoint;
  /// WS-interface endpoint
  std::string arex_endpoint;

  /// Use Python backends, else Bash
  bool use_python_lrms;
  /// Struct for ssh options (only suppored by Python backends)
  SSHConfig ssh_config;

  /// Logger object
  static Arc::Logger logger;

  /// Set defaults for all configuration parameters. Called by constructors.
  void SetDefaults();
  bool ValidateSSH();

  /// Assignment operator and copy constructor are private to prevent copying.
  GMConfig& operator=(const GMConfig& conf);
  GMConfig(const GMConfig& conf);
};

} // namespace ARex

#endif /* GMCONFIG_H_ */
