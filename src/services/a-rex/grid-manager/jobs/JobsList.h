#ifndef GRID_MANAGER_STATES_H
#define GRID_MANAGER_STATES_H

#include <sys/types.h>
#include <list>

#include "../conf/StagingConfig.h"

#include "GMJob.h"
#include "JobDescriptionHandler.h"
#include "PythonPlugin.h"

namespace ARex {

class JobFDesc;
class DTRGenerator;
class GMConfig;

/// ZeroUInt is a wrapper around unsigned int. It provides a consistent default
/// value, as int type variables have no predefined value assigned upon
/// creation. It also protects from potential counter underflow, to stop
/// counter jumping to MAX_INT. TODO: move to common lib?
class ZeroUInt {
private:
 unsigned int value_;
public:
 ZeroUInt(void):value_(0) { };
 ZeroUInt(unsigned int v):value_(v) { };
 ZeroUInt(const ZeroUInt& v):value_(v.value_) { };
 ZeroUInt& operator=(unsigned int v) { value_=v; return *this; };
 ZeroUInt& operator=(const ZeroUInt& v) { value_=v.value_; return *this; };
 ZeroUInt& operator++(void) { ++value_; return *this; };
 ZeroUInt operator++(int) { ZeroUInt temp(value_); ++value_; return temp; };
 ZeroUInt& operator--(void) { if(value_) --value_; return *this; };
 ZeroUInt operator--(int) { ZeroUInt temp(value_); if(value_) --value_; return temp; };
 operator unsigned int(void) const { return value_; };
};


/// List of jobs. This class contains the main job management logic which moves
/// jobs through the state machine. New jobs found through Scan methods are
/// held in memory until reaching FINISHED state.
class JobsList {
 public:
  typedef std::list<GMJob>::iterator iterator;
 private:
  PythonPlugin py;
  // List of jobs currently tracked in memory
  std::list<GMJob> jobs;
  // GM configuration
  const GMConfig& config;
  // Staging configuration
  StagingConfig staging_config;
  // Dir containing finished/deleted jobs which is scanned in ScanOldJobs.
  // Since this can happen over multiple calls a pointer is kept as a member
  // variable so scanning picks up where it finished last time.
  Glib::Dir* old_dir;
  // Generator for handling data staging
  DTRGenerator* dtr_generator;
  // Job description handler
  JobDescriptionHandler job_desc_handler;
  // number of jobs for every state
  int jobs_num[JOB_STATE_NUM];
  // map of number of active jobs for each DN
  std::map<std::string, ZeroUInt> jobs_dn;
  // number of jobs currently in pending state
  int jobs_pending;

  // Add job into list without checking if it is already there.
  // 'i' will be set to iterator pointing at new job
  bool AddJobNoCheck(const JobId &id,iterator &i,uid_t uid,gid_t gid);
  // Add job into list without checking if it is already there
  bool AddJobNoCheck(const JobId &id,uid_t uid,gid_t gid);
  // Perform all actions necessary in case of job failure
  bool FailedJob(const iterator &i,bool cancel);
  // Remove Job from list. All corresponding files are deleted and pointer is
  // advanced. If finished is false - job is not destroyed if it is FINISHED
  // If active is false - job is not destroyed if it is not UNDEFINED. Returns
  // false if external process is still running.
  bool DestroyJob(iterator &i,bool finished=true,bool active=true);
  // Perform actions necessary in case job goes to/is in SUBMITTING/CANCELING state
  bool state_submitting(const iterator &i,bool &state_changed,bool cancel=false);
  // Same for PREPARING/FINISHING
  bool state_loading(const iterator &i,bool &state_changed,bool up);
  // Returns true if job is waiting on some condition or limit before
  // progressing to the next state
  bool JobPending(JobsList::iterator &i);
  // Get the state in which the job failed from .local file
  job_state_t JobFailStateGet(const iterator &i);
  // Write the state in which the job failed to .local file
  bool JobFailStateRemember(const iterator &i,job_state_t state,bool internal = true);
  // In case of job restart, recreates lists of input and output files taking
  // into account what was already transferred
  bool RecreateTransferLists(const JobsList::iterator &i);
  // Read into ids all jobs in the given dir
  bool ScanJobs(const std::string& cdir,std::list<JobFDesc>& ids);
  // Read into ids all jobs in the given dir with marks given by suffices
  // (corresponding to file suffixes)
  bool ScanMarks(const std::string& cdir,const std::list<std::string>& suffices,std::list<JobFDesc>& ids);
  // Called after service restart to move jobs that were processing to a
  // restarting state
  bool RestartJobs(const std::string& cdir,const std::string& odir);
  // Release delegation after job finishes
  void UnlockDelegation(JobsList::iterator &i);
  // Calculate job expiration time from last state change and configured lifetime
  time_t PrepareCleanupTime(JobsList::iterator &i, time_t& keep_finished);
  // Read in information from .local file
  bool GetLocalDescription(const JobsList::iterator &i);

  // Main job processing method. Analyze current state of job, perform
  // necessary actions and advance state or remove job if needed. Iterator 'i'
  // is advanced or erased inside this function.
  bool ActJob(iterator &i);

  // ActJob() calls one of these methods depending on the state of the job.
  // Parameters:
  //   once_more - if true then ActJob should process this job again
  //   delete_job - if true then ActJob should delete the job
  //   job_error - if true then an error happened in this processing state
  //   state_changed - if true then the job state was changed
  void ActJobUndefined(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobAccepted(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobPreparing(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobSubmitting(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobCanceling(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobInlrms(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobFinishing(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobFinished(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);
  void ActJobDeleted(iterator &i,bool& once_more,bool& delete_job,bool& job_error,bool& state_changed);

 public:
  // Constructor.
  JobsList(const GMConfig& gmconfig);
  // std::list methods for using JobsList like a regular list
  iterator begin(void) { return jobs.begin(); };
  iterator end(void) { return jobs.end(); };
  size_t size(void) const { return jobs.size(); };
  iterator erase(iterator& i) { return jobs.erase(i); };

  // Return iterator to object matching given id or jobs.end() if not found
  iterator FindJob(const JobId &id);
  // Information about jobs for external utilities
  // No of jobs in all active states from ACCEPTED and FINISHING
  int AcceptedJobs() const;
  // No of jobs in batch system or in process of submission to batch system
  int RunningJobs() const;
  // No of jobs in data staging
  int ProcessingJobs() const;
  // No of jobs staging in data before job execution
  int PreparingJobs() const;
  // No of jobs staging out data after job execution
  int FinishingJobs() const;

  // Python
  void SetPython(PythonPlugin& py) { py = py; };
  // Set DTR Generator for data staging
  void SetDataGenerator(DTRGenerator* generator) { dtr_generator = generator; };
  // Call ActJob for all current jobs
  bool ActJobs(void);
  // Look for new or restarted jobs. Jobs are added to list with state UNDEFINED
  bool ScanNewJobs(void);
  // Collect all jobs in all states
  bool ScanAllJobs(void);
  // Pick jobs which have been marked for restarting, cancelling or cleaning
  bool ScanNewMarks(void);
  // Look for finished or deleted jobs and process them. Jobs which are
  // restarted will be added back into the main processing loop. This method
  // can be limited in the time it can run for and number of jobs it can scan.
  // It returns false if failed or scanning finished.
  bool ScanOldJobs(int max_scan_time,int max_scan_jobs);
  // Add job with specified id. 
  // Returns true if job was found and added.
  bool AddJob(const JobId& id);
  // Rearrange status files on service restart
  bool RestartJobs(void);
  // Send signals to external processes to shut down nicely (not implemented)
  void PrepareToDestroy(void);
};

} // namespace ARex

#endif
