#!/bin/bash
source include/bash_header.sh

TARGET="unix"

mkdir -p ext
cd ext
EXT_DIR=${PWD}

# If we are running on a Debian-based system, a couple of dependencies
# are packaged, so we prompt the user to allow us to install them.
# Currently, we support Ubuntu and Debian.
BASE_PKGS="wget subversion autoconf zlib1g-dev python-pip libcurl4-gnutls-dev libtbb-dev libevent-dev libnuma-dev libjansson-dev"
BOOST_PKGS="libboost-math-dev libboost-system-dev libboost-thread-dev libboost-regex-dev libboost-filesystem-dev libboost-program-options-dev libboost-iostreams-dev mpich2"
ANTLR_PKGS="antlr3 libantlr3c-dev"
CTEMPLATE_PKGS="libctemplate-dev"
GOOGLE_PKGS="libgflags-dev libprotobuf-dev libprotobuf-c0-dev protobuf-c-compiler libgoogle-perftools-dev protobuf-compiler"
#HDFS_PKGS="libhdfs0 libhdfs0-dev"
HDFS_PKGS=""
MONO_PKGS="mono-mcs mono-xbuild mono-utils mono-runtime-sgen mono-runtime-common mono-runtime mono-mcs mono-dmcs mono-devel libmono-profiler mono-gmcs"

UBUNTU_PKGS="${BASE_PKGS} ${BOOST_PKGS} ${ANTLR_PKGS} ${CTEMPLATE_PKGS} ${GOOGLE_PKGS} ${HDFS_PKGS} ${MONO_PKGS}"
DEBIAN_PKGS="${BASE_PKGS} ${BOOST_PKGS} ${ANTLR_PKGS} ${CTEMPLATE_PKGS} ${GOOGLE_PKGS} ${HDFS_PKGS} ${MONO_PKGS}"

GLOG_VER="HEAD"
#BOOST_VER="1.46.0"
GFLAGS_VER="2.0-1"
PROTOBUF_VER="2.4.1"

################################################################################

function get_arch() {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "i386"
  elif [[ $1 == "amd64" || $1 == "x86_64" ]]; then
    echo "amd64"
  else
    echo "unknown"
  fi
}

function get_archx {
  if [[ $1 == "i368" || $1 == "i468" || $1 == "i568" || $1 == "i686" || $1 == "IA-32" ]]; then
    echo "x86"
  elif [[ $1 == "amd64" || $1 == "x86_64" ]]; then
    echo "x86_64"
  else
    echo "unknown"
  fi
}

################################################################################

function check_os_release_compatibility() {
  print_subhdr "OS COMPATIBILITY CHECK ($1 $2)"
  if [[ $1 == "Ubuntu" ]]; then
    echo -n "$1 $2 is compatible."
    echo_success
    echo
  elif [[ $1 == "Debian" ]]; then
    echo -n "$1 $2 is compatible."
    echo_success
    echo
    echo "WARNING: Running Musketeer on Debian is currently not well tested." \
      "YMMV!"
    ask_continue
  else
    echo_failure
    echo "Unsupported OS! Proceed at your own risk..."
    ask_continue
  fi
}

################################################################################

function check_dpkg_packages() {
  print_subhdr "$1 PACKAGE CHECK"
  if [[ $1 == "Ubuntu" ]]; then
    OS_PKGS=${UBUNTU_PKGS}
  else
    OS_PKGS=${DEBIAN_PKGS}
  fi
  for i in ${OS_PKGS}; do
    PKG_RES=$(dpkg-query -W -f='${Status}\n' ${i} | grep -E "^install" 2>/dev/null)
    if [[ $PKG_RES == "" ]]; then
#    if [ $(echo $PKG_RES | grep "No package") ]; then
      MISSING_PKGS="${MISSING_PKGS} ${i}"
    fi
  done

  if [[ $MISSING_PKGS != "" ]]; then
    echo -n "The following packages are required to run ${PROJECT}, "
    echo "but are not currently installed: "
    echo ${MISSING_PKGS}
    echo
    echo "Please install them using the following commmand: "
    echo "$ sudo apt-get install${MISSING_PKGS}"
    echo
    exit 1
  else
    echo -n "All required Ubuntu packages are installed."
    echo_success
    echo
    touch .$1-ok
  fi
}

################################################################################

function get_dep_svn {
  REPO=$2
  NAME=$1
  if [[ ${REPO} == "googlecode" ]]; then
    REPO="http://${NAME}.googlecode.com/svn/trunk/"
  fi

  if [ -d ${NAME}-svn ]
  then
    svn up ${NAME}-svn/
  else
    mkdir -p ${NAME}-svn
    svn co ${REPO} ${NAME}-svn/
  fi
}

################################################################################

function get_dep_deb {
  URL=$2
  NAME=$1
  FILE=$(basename ${URL})
  wget -q -N ${URL}
  if [ ${NAME}-timestamp -ot ${FILE} ]
  then
#    sudo dpkg -i ${FILE}
    touch -r ${FILE} ${NAME}-timestamp
  fi
}

################################################################################

function get_dep_git {
  REPO=$2
  NAME=$1

  if [ -d ${NAME}-git ]
  then
    svn up ${NAME}-git/
  else
    mkdir -p ${NAME}-git
    git clone ${REPO} ${NAME}-git/
  fi
}

################################################################################

function get_dep_arch {
  URL=$2
  NAME=$1
  FILE=$(basename ${URL})
  wget -q -N ${URL}
  tar -xzf ${FILE}
  if [ ${NAME}-timestamp -ot ${FILE} ]
  then
    touch -r ${FILE} ${NAME}-timestamp
  fi
}

################################################################################

function get_dep_wget {
  URL=$2
  NAME=$1
  FILE=$(basename ${URL})
  wget -q -N ${URL}
  if [ ${NAME}-timestamp -ot ${FILE} ]
  then
    touch -r ${FILE} ${NAME}-timestamp
  fi
}

################################################################################

print_hdr "FETCHING & INSTALLING EXTERNAL DEPENDENCIES"

OS_ID=$(lsb_release -i -s)
OS_RELEASE=$(lsb_release -r -s)
ARCH_UNAME=$(uname -m)
ARCH=$(get_arch "${ARCH_UNAME}")
ARCHX=$(get_archx "${ARCH_UNAME}")

SCC_CC_SCRIPT="/opt/compilerSetupFiles/crosscompile.sh"

# N.B.: We explicitly exclude the SCC here since we need to build on the MCPC
# in the case of the SCC. The MCPC runs 64-bit Ubuntu, but the SCC cores run
# some custom stripped-down thing that does not have packages.
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  echo "Detected $OS_ID..."
  check_os_release_compatibility ${OS_ID} ${OS_RELEASE}
  if [ -f "${OS_ID}-ok" ]; then
    echo -n "${OS_ID} package check previously ran successfully; skipping. "
    echo "Delete .${OS_ID}-ok file if you want to re-run it."
  else
    echo "Checking if necessary packages are installed..."
    check_dpkg_packages ${OS_ID}
  fi
elif [[ ${TARGET} == "scc" ]]; then
  echo "Building for the SCC. Note that you MUST build on the MCPC, and "
  echo "that ${SCC_CC_SCRIPT} MUST exist and be accessible."
  ask_continue
  source ${SCC_CC_SCRIPT}
else
  echo "Operating systems other than Ubuntu (>=10.04) and Debian are not"
  echo "currently supported for automatic configuration."
  exit 0
fi

## Google Gflags command line flag library
print_subhdr "GOOGLE GFLAGS LIBRARY"
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  PKG_RES1=$(dpkg-query -l | grep "libgflags0" 2>/dev/null)
  PKG_RES2=$(dpkg-query -l | grep "libgflags-dev" 2>/dev/null)
  if [[ $PKG_RES1 != "" && $PKG_RES2 != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  else
    echo -n "libgflags not installed."
    echo_failure
    echo "Please install the libgflags-dev package."
    echo
    # will bring in the other package via ddependency
    echo "$ sudo apt-get install libgflags-dev"
    exit 1
 fi
else
  # non-deb OS -- need to get tarball and extract, config, make & install
  echo "Downloading and extracting release tarball for Google gflags library..."
  GFLAGS_BUILD_DIR=${EXT_DIR}/google-gflags-build
  mkdir -p ${GFLAGS_BUILD_DIR}
  get_dep_arch "google-gflags" "http://gflags.googlecode.com/files/gflags-${GFLAGS_VER}.tar.gz"
  cd gflags-${GFLAGS_VER}
  echo -n "Building google-gflags library..."
  RES=$(./configure --prefix=${GFLAGS_BUILD_DIR} && make --quiet && make --quiet install 2>/dev/null)
  print_succ_or_fail $RES
  cd ${EXT_DIR}
fi

## Google Log macros
## N.B.: This must go *after* gflags, since glog will notice that gflags is
## installed, and produce extra options (default flags like --logtostderr).
print_subhdr "GOOGLE GLOG LIBRARY"
GLOG_DIR=google-glog-git
GLOG_INSTALL_FILE="/usr/lib/pkgconfig/libglog.pc"
PKG_RES=$(dpkg-query -l | grep "libgoogle-glog-dev" 2>/dev/null)
#GLOG_BUILD_DIR=${EXT_DIR}/google-glog-build
#mkdir -p ${GLOG_BUILD_DIR}
if [[ ${TARGET} == "scc" || ( ${PKG_RES} == "" && ! -f ${GLOG_INSTALL_FILE} ) ]]; then
  get_dep_git "google-glog" "https://github.com/google/glog"
  cd ${GLOG_DIR}
  echo -n "Building google-glog library..."
  if [[ ${TARGET} == "scc" ]]; then
    RES=$(./configure --prefix=${GLOG_BUILD_DIR} && make --quiet && make --quiet install 2>/dev/null)
  else
    RES=$(./configure --prefix=/usr && make --quiet 2>/dev/null)
  fi
  print_succ_or_fail $RES
  if [[ ${TARGET} != "scc" ]]; then
    echo "google-glog library (v${GLOG_VER}) was built in ${GLOG_DIR}. "
    echo "Please run the following comamnds to install it: "
    echo
    echo "$ cd ${EXT_DIR}/${GLOG_DIR}"
    echo "$ sudo make install"
    echo
    echo "... and then re-run."
    exit 1
  fi
else
  echo -n "Already installed!"
  print_succ_or_fail 0
fi
cd ${EXT_DIR}


## Boost
# print_subhdr "BOOST C++ LIBRARIES"
# if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
# then
#   PKG_RES=$(dpkg-query -l | grep "libboost-dev" 2>/dev/null)
#   if [[ ${PKG_RES} != "" ]]; then
#     echo -n "Already installed."
#     echo_success
#     echo
#   fi
# else
#   BOOST_VER_US=$(echo ${BOOST_VER} | sed 's/\./_/g')
#   # Get Boost release archive
#   echo "Downloading and extracting Boost ${BOOST_VER}..."
#   get_dep_arch "boost" "http://downloads.sourceforge.net/project/boost/boost/${BOOST_VER}/boost_${BOOST_VER_US}.t\
# ar.gz"
#   mkdir -p ${EXT_DIR}/boost-build
#   BOOST_EXTRACT_DIR=${EXT_DIR}/boost_${BOOST_VER_US}
#   cd ${BOOST_EXTRACT_DIR}
#   echo "Building..."
#   BOOST_BUILD_DIR=${EXT_DIR}/boost-build
#   ${BOOST_EXTRACT_DIR}/bootstrap.sh --prefix=${BOOST_BUILD_DIR}
#   echo "Installing... (This may take a long time!)"
#   echo
#   ${BOOST_EXTRACT_DIR}/b2 -d0 install
#   echo_success
#   echo
# fi

## Hadoop
print_subhdr "Hadoop jars"
get_dep_wget "hadoop-common" "http://www.cl.cam.ac.uk/~icg27/hadoop-common.jar"
get_dep_wget "hadoop-core" "http://www.cl.cam.ac.uk/~icg27/hadoop-core.jar"
get_dep_wget "hadoop-hdfs" "http://www.cl.cam.ac.uk/~icg27/hadoop-hdfs.jar"

## Install pystache
PYSTACHE_INSTALL_FILE="/usr/local/bin/pystache"
if [[ ! -f ${PYSTACHE_INSTALL_FILE} ]]; then
  echo "Please install pystache using the following commmand: "
  echo "$ sudo pip install pystache ; sudo rm -r build/pystache/"
  echo "Please rerun the fetch-externals script after instalation"
  exit 1
fi

## Protocol buffers
print_subhdr "GOOGLE PROTOCOL BUFFERS"
if [[ ${TARGET} != "scc" && ( ${OS_ID} == "Ubuntu" || ${OS_ID} == "Debian" ) ]];
then
  PKG_RES1=$(dpkg-query -l | grep "libprotobuf" 2>/dev/null)
  PKG_RES2=$(dpkg-query -l | grep "protobuf-compiler" 2>/dev/null)
  if [[ ${PKG_RES1} != "" && ${PKG_RES2} != "" ]]; then
    echo -n "Already installed."
    echo_success
    echo
  fi
else
  # Get protobufs release archive
  echo "Downloading and extracting Google protocol buffers ${PROTOBUF_VER}..."
  get_dep_arch "protobuf" "http://protobuf.googlecode.com/files/protobuf-${PROTOBUF_VER}.tar.gz"
  mkdir -p ${EXT_DIR}/protobuf-build
  PROTOBUF_EXTRACT_DIR=${EXT_DIR}/protobuf-${PROTOBUF_VER}
  cd ${PROTOBUF_EXTRACT_DIR}
  PROTOBUF_BUILD_DIR=${EXT_DIR}/protobuf-build
  echo -n "Building..."
  RES=$(./configure --prefix=${PROTOBUF_BUILD_DIR} && make --quiet && make --quiet check && make --quiet install 2>/dev/null)
  print_succ_or_fail $RES
  cd ${EXT_DIR}
fi

## Cake (for building Wildcherry operators)
print_subhdr "CAKE BUILD TOOL"
get_dep_git "cake" "https://github.com/Zomojo/Cake.git"
cd cake-git
RES=$(ls cake)
print_succ_or_fail ${RES}
cd ${EXT_DIR}


## cpplint.py (linter script)
print_subhdr "CPPLINT HELPER SCRIPT"
get_dep_wget "cpplint" "http://google-styleguide.googlecode.com/svn/trunk/cpplint/cpplint.py"


touch ${EXT_DIR}/.ext-ok
