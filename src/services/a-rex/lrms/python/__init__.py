"""
The ``lrms`` package contains batch system specific modules and a sub-package ``lrms.common``. Each module has functions for cancel, scan and submit jobs, with an interface similar to the bash backend scripts:

* Cancel ( arc.conf, grami_file )
* Scan   ( arc.conf, control_dirs )
* Submit ( arc.conf, job_description, job_container )

"""
