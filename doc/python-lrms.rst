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

Package: lrms >
---------------

.. automodule:: lrms

....

Module: lrms.fork
+++++++++++++++++
.. automodule:: lrms.fork
    :members:

|

....

|

Module: lrms.lsf
++++++++++++++++

.. automodule:: lrms.lsf
    :members:

|

....

|

Module: lrms.slurm
++++++++++++++++++
.. automodule:: lrms.slurm
    :members:

|

....

|

Package: lrms.common >
----------------------

.. automodule:: lrms.common

....

Module: lrms.common.cancel
++++++++++++++++++++++++++
.. automodule:: lrms.common.cancel
    :members:

|

....

|

Module: lrms.common.common
++++++++++++++++++++++++++
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

Module: lrms.common.config
++++++++++++++++++++++++++
.. automodule:: lrms.common.config
    :members:

|

....

|

Module: lrms.common.files
+++++++++++++++++++++++++
.. automodule:: lrms.common.files
    :members:

|

....

|

Module: lrms.common.parse
+++++++++++++++++++++++++
.. automodule:: lrms.common.parse
    :members:

|

....

|

Module: lrms.common.proc
++++++++++++++++++++++++
.. automodule:: lrms.common.proc
    :members:

|

....

|

Module: lrms.common.scan
++++++++++++++++++++++++
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

Module: lrms.common.submit
++++++++++++++++++++++++++
.. automodule:: lrms.common.submit
    :members:

|

....

|

Module: lrms.common.tools
+++++++++++++++++++++++++
.. automodule:: lrms.common.tools
    :members:

|
