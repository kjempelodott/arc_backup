package SLURMmodPy;

# In order to run this module the Inline::Python Perl module must be installed.
# It could be installed in the home directory and then the PERL5LIB environment
# variable should point to this location.

use strict;
use POSIX qw(ceil floor);

use Inline Python => <<"END_OF_PYTHON_CODE";
import SLURMmod

def get_lrms_options_schema():
    return SLURMmod.get_lrms_options_schema()

def get_lrms_info(options):
    try:
        return SLURMmod.get_lrms_info(options)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e

END_OF_PYTHON_CODE

our @ISA = ('Exporter');

# Module implements these subroutines for the LRMS interface
our @EXPORT_OK = ('get_lrms_info', 'get_lrms_options_schema');

Inline->init();

1;
