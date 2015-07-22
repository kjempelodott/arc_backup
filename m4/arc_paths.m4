dnl
dnl Substitite some relative paths
dnl

AC_DEFUN([ARC_RELATIVE_PATHS],
[
  AC_REQUIRE([ARC_RELATIVE_PATHS_INIT])
  AC_REQUIRE([AC_LIB_PREPARE_PREFIX])

  AC_LIB_WITH_FINAL_PREFIX([
      eval instprefix="\"${exec_prefix}\""
      eval arc_libdir="\"${libdir}\""
      eval arc_initddir="\"${initddir}\""
      eval arc_bindir="\"${bindir}\""
      eval arc_sbindir="\"${sbindir}\""
      eval arc_pkglibdir="\"${libdir}/arc\""
      eval arc_pkglibexecdir="\"${libexecdir}/arc\""
      # It seems arc_datadir should be evaluated twice to be expanded fully.
      eval arc_datadir="\"${datadir}/arc\""
      eval arc_datadir="\"${arc_datadir}\""
  ])

  libsubdir=`get_relative_path "$instprefix" "$arc_libdir"`
  initdsubdir=`get_relative_path "$instprefix" "$arc_initddir"`
  pkglibsubdir=`get_relative_path "$instprefix" "$arc_pkglibdir"`
  pkglibexecsubdir=`get_relative_path "$instprefix" "$arc_pkglibexecdir"`
  pkgdatasubdir=`get_relative_path "$instprefix" "$arc_datadir"`
  pkglibdir_rel_to_pkglibexecdir=`get_relative_path "$arc_pkglibexecdir" "$arc_pkglibdir"`
  sbindir_rel_to_pkglibexecdir=`get_relative_path "$arc_pkglibexecdir" "$arc_sbindir"`
  bindir_rel_to_pkglibexecdir=`get_relative_path "$arc_pkglibexecdir" "$arc_bindir"`
  pkgdatadir_rel_to_pkglibexecdir=`get_relative_path "$arc_pkglibexecdir" "$arc_datadir"`

  AC_MSG_NOTICE([pkglib subdirectory is: $pkglibsubdir])
  AC_MSG_NOTICE([pkglibexec subdirectory is: $pkglibexecsubdir])
  AC_MSG_NOTICE([relative path of pkglib to pkglibexec is: $pkglibdir_rel_to_pkglibexecdir])

  AC_SUBST([libsubdir])
  AC_SUBST([initdsubdir])
  AC_SUBST([pkglibsubdir])
  AC_SUBST([pkglibexecsubdir])
  AC_SUBST([pkglibdir_rel_to_pkglibexecdir])
  AC_SUBST([sbindir_rel_to_pkglibexecdir])
  AC_SUBST([bindir_rel_to_pkglibexecdir])
  AC_SUBST([pkgdatadir_rel_to_pkglibexecdir])
  AC_SUBST([pkgdatasubdir])

  AC_DEFINE_UNQUOTED([INSTPREFIX], ["${instprefix}"], [installation prefix])
  AC_DEFINE_UNQUOTED([LIBSUBDIR], ["${libsubdir}"], [library installation subdirectory])
  AC_DEFINE_UNQUOTED([INITDSUBDIR], ["${initdsubdir}"], [init scripts subdirectory])
  AC_DEFINE_UNQUOTED([PKGLIBSUBDIR], ["${pkglibsubdir}"], [plugin installation subdirectory])
  AC_DEFINE_UNQUOTED([PKGLIBEXECSUBDIR], ["${pkglibexecsubdir}"], [helper programs installation subdirectory])
  AC_DEFINE_UNQUOTED([PKGDATASUBDIR], ["${pkgdatasubdir}"], [package data subdirectory])

])

AC_DEFUN([ARC_RELATIVE_PATHS_INIT],
[
  get_relative_path() {
    olddir=`echo $[]1 | sed -e 's|/+|/|g' -e 's|^/||' -e 's|/*$|/|'`
    newdir=`echo $[]2 | sed -e 's|/+|/|g' -e 's|^/||' -e 's|/*$|/|'`

    O_IFS=$IFS
    IFS=/
    relative=""
    common=""
    for i in $olddir; do
      if echo "$newdir" | grep -q "^$common$i/"; then
        common="$common$i/"
      else
        relative="../$relative"
      fi
    done
    IFS=$O_IFS
    echo $newdir | sed "s|^$common|$relative|" | sed 's|/*$||'
  }
])

