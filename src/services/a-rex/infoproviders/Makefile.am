pythondir = $(PYTHON_SITE_LIB)

python_PYTHON = SLURMmod.py LSFmod.py LRMSmod.py

pkgdata_SCRIPTS = CEinfo.pl se.pl
dist_pkgdata_SCRIPTS = glue-generator.pl \
	grid-info-soft-register glite-info-provider-ldap

dist_pkgdata_DATA = ARC0mod.pm SGEmod.pm FORKmod.pm PBS.pm DGBridge.pm \
	LL.pm LSF.pm Condor.pm condor_env.pm SLURM.pm SLURMmod.pm Boinc.pm \
	IniParser.pm LogUtils.pm Sysinfo.pm \
	LRMSInfo.pm GMJobsInfo.pm HostInfo.pm RTEInfo.pm \
	InfoChecker.pm ConfigCentral.pm \
	ARC0ClusterInfo.pm ARC1ClusterInfo.pm \
	SGE.pm Fork.pm Shared.pm \
	XmlPrinter.pm GLUE2xmlPrinter.pm \
	LdifPrinter.pm GLUE2ldifPrinter.pm \
	NGldifPrinter.pm InfosysHelper.pm \
	LSFmodPy.pm SLURMmodPy.pm

arcldapschemadir = $(pkgdatadir)/ldap-schema
dist_arcldapschema_DATA = schema/nordugrid.schema

PERL = @PERL@

PERLSCRIPTS = $(dist_pkgdata_DATA) CEinfo.pl
TESTS_ENVIRONMENT = $(PERL) -I$(srcdir) -Mstrict -wc
TESTS = $(PERLSCRIPTS)

check_SCRIPTS = $(PERLSCRIPTS)

# Check if BDI module is available, if not exclude Boinc.pm from TESTS variable.
Boinc.pm: FORCE
	$(eval TESTS := $(shell if `$(PERL) -e "use BDI; exit;" > /dev/null 2>&1`; then echo "$(TESTS)"; else echo "$(TESTS)" | sed 's/Boinc.pm//'; fi))
FORCE:

SUBDIRS = test
