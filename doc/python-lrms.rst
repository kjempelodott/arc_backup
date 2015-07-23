Module tree >
=============

:mod:`lrms <lrms>`
 | |– :mod:`fork <lrms.fork>`
 | |– :mod:`lsf <lrms.lsf>`
 | |– :mod:`slurm <lrms.slurm>`
 | |– :mod:`common <lrms.common>`
 |      |– :mod:`cancel <lrms.common.cancel>`
 |      |– :mod:`common <lrms.common.common>`
 |      |– :mod:`config <lrms.common.config>`
 |      |– :mod:`files <lrms.common.files>`
 |      |– :mod:`parse <lrms.common.parse>`
 |      |– :mod:`proc <lrms.common.proc>`
 |      |– :mod:`scan <lrms.common.scan>`
 |      |– :mod:`submit <lrms.common.submit>`
 |      |– :mod:`tools <lrms.common.tools>`

|
|

The lrms package >
------------------

.. automodule:: lrms

....

The lrms.fork module
++++++++++++++++++++
.. automodule:: lrms.fork
    :members:

|

....

|

The lrms.lsf module
+++++++++++++++++++

.. automodule:: lrms.lsf
    :members:

|

....

|

The lrms.slurm module
+++++++++++++++++++++
.. automodule:: lrms.slurm
    :members:

|

....

|

The lrms.common package >
-------------------------

.. automodule:: lrms.common

....

The lrms.common.cancel module
+++++++++++++++++++++++++++++
.. automodule:: lrms.common.cancel
    :members:

|

....

|

The lrms.common.common module
+++++++++++++++++++++++++++++
.. automodule:: lrms.common.common
    :members:

.. py:data:: Config

    :py:class:`Object` object holding arc.conf options

.. py:data:: Logger

    :py:class:`arc.Logger (Swig Object of type 'Arc::Logger *')`

.. py:data:: UserConfig

    :py:class:`arc.UserConfig (Swig Object of type 'Arc::UserConfig *')`

|

....

|

The lrms.common.config module
+++++++++++++++++++++++++++++
.. automodule:: lrms.common.config
    :members:

|

....

|

The lrms.common.files module
++++++++++++++++++++++++++++
.. automodule:: lrms.common.files
    :members:

|

....

|

The lrms.common.parse module
++++++++++++++++++++++++++++
.. automodule:: lrms.common.parse
    :members:

|

....

|

The lrms.common.proc module
+++++++++++++++++++++++++++
.. automodule:: lrms.common.proc
    :members:

|

....

|

The lrms.common.scan module
+++++++++++++++++++++++++++
.. automodule:: lrms.common.scan
    :members:

.. py:data:: RUNNING

    :py:obj:`list` of possible states (:py:obj:`str`) for running jobs

.. py:data:: UID

    user ID (:py:obj:`int`)

.. py:data:: UID

    group ID (:py:obj:`int`)

.. py:data:: MESSAGES

    mapping between job states and job messages (:py:obj:`dict` { :py:obj:`str` : :py:obj:`str` })

|

....

|

The lrms.common.submit module
+++++++++++++++++++++++++++++
.. automodule:: lrms.common.submit
    :members:

|

....

|

The lrms.common.tools module
++++++++++++++++++++++++++++
.. automodule:: lrms.common.tools
    :members:

|
