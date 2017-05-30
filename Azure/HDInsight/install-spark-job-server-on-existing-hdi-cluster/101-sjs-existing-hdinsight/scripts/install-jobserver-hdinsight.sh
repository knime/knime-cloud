#!/bin/bash
#
# Versions are auto detected if not defined in ENV.
# Run installer with custom versions:
#   KNIME_VERSION="3.3" SPARK_VERSION="1.6" JS_VERSION="0.6.2.1" sh install-jobserver-emr.sh
#

if [ "$UID" != "0" ] ; then
  echo "Script must be run as root!"
  exit 1
fi

CONTEXTJVM=$1
NUMCORES=$2
MEMORYPERNODE=$3

if [ -z "$NUMCORES" ] ; then
 NUMCORES="4"
fi
if [ -z "$CONTEXTJVM" ] ; then
 CONTEXTJVM="true"
fi
if [ -z "$MEMORYPERNODE" ] ; then
 MEMORYPERNODE="8G"
fi

CLEARTMP=""
CLEARLOG=""
DOWNLOAD="true"
JSBUILD==""
INSTDIR=/opt
KNIME_VERSION="3.3"
JS_VERSION="0.6.2.1"

# Identify spark version and download job server
if [ -n "$DOWNLOAD" ] ; then
  [ -z "$JS_VERSION" ] && { echo "Environment variable JS_VERSION not set." ; echo "${USAGE}" ; exit 1 ; }
  [ -z "$KNIME_VERSION" ] && { echo "Environment variable KNIME_VERSION not set." ; echo "${USAGE}" ; exit 1 ; }

  CONF_SUFFIX=""
  if [ -n "${CONF_TAG}" ] ; then
    CONF_SUFFIX=".${CONF_TAG}"
  fi

  JSBUILD="/tmp/spark-job-server.tar.gz"
  ENV_CONF="/tmp/spark-job-server-environment.conf"
  BASE_URL="https://download.knime.org/store/$KNIME_VERSION"
  JOB_SERVER_URL="https://download.knime.org/store/3.3/spark-job-server-0.6.2.1-KNIME_hdi-3.5.tar.gz"
  ENV_CONF_URL="https://download.knime.org/store/3.3/spark-job-server-0.6.2.1-KNIME_hdi_environment.conf"

  echo "Downloading job server from: $JOB_SERVER_URL"
  wget -q -O $JSBUILD "$JOB_SERVER_URL"
  echo "Downloading environment.conf from: $ENV_CONF_URL"
  wget -q -O $ENV_CONF "$ENV_CONF_URL"
fi

[ -f "$JSBUILD" ] || { echo "$JSBUILD does not exist" ; exit 1 ; }
[[ "$JSBUILD" =~ ^.*\.tar\.gz$ ]] || { echo "$JSBUILD does not exist" ; exit 1 ; }
[ -f "$ENV_CONF" ] || { echo "$ENV_CONF does not exist" ; exit 1 ; }

###########  sed variables ######################################################
sed -i -e "0,/.*context-per-jvm =.*/s/.*context-per-jvm =.*/context-per-jvm = $CONTEXTJVM/g" $ENV_CONF
sed -i -e "0,/.*num-cpu-cores =.*/s/.*num-cpu-cores =.*/num-cpu-cores = $NUMCORES/g" $ENV_CONF
sed -i -e "0,/.*memory-per-node =.*/s/.*memory-per-node =.*/memory-per-node = $MEMORYPERNODE/g" $ENV_CONF
###########  /sed variables #####################################################

if [ -e "$INSTDIR/spark-job-server" ] ; then
  echo "Stopping spark-job-server"

  # stop any running jobserver
  command -v  systemctl >/dev/null 2>&1 && { systemctl stop spark-job-server ; }
  command -v  systemctl >/dev/null 2>&1 || { /etc/init.d/spark-job-server stop ; }

  # backup old installation files
  BAKDIR="/root/install-jobserver-backup-$( date --rfc-3339=seconds | sed 's/ /_/' )"
  mkdir -p "$BAKDIR"

  if [ -L "$INSTDIR/spark-job-server" ] ; then
    rm "$INSTDIR/spark-job-server"
  fi

  echo "Backing up spark-job-server installation(s) to $BAKDIR"
  # move any old jobserver files into backup area
  mv "$INSTDIR"/spark-job-server* "$BAKDIR/"

  [ -n "$CLEARTMP" ] && { rm -Rf /tmp/spark-job-server/ ; rm -Rf /tmp/spark-jobserver/ ; }
  [ -n "$CLEARLOG" ] && { rm -Rf /var/log/spark-job-server/ ; }
fi

pushd $(mktemp -d) > /dev/null
tar -xzf "$JSBUILD" -C "$PWD"/
JSDIR="$(basename ./*)"
[ -n "$ENV_CONF" ] && mv $ENV_CONF $JSDIR/environment.conf
mv "$JSDIR" "$INSTDIR/"
rm -R $PWD
popd > /dev/null

pushd "$INSTDIR" > /dev/null

id $JOBSERVER_USER >/dev/null 2>&1 || { useradd -U -M -s /bin/false -d ${INSTDIR}/${JSDIR} $JOBSERVER_USER; }

JOBSERVER_USER="spark-job-server"
# create user to be used for spark job server
if [ -z $(getent passwd spark-job-server) ] ; then
   useradd -U -M -s /bin/false -d /opt/spark-job-server $JOBSERVER_USER
fi

sed -r "s#JSDIR#${INSTDIR}/${JSDIR}#g" -i $JSDIR/spark-job-server.service


if [ -L "$INSTDIR/spark-job-server" ] ; then
  rm "$INSTDIR/spark-job-server"
fi
ln -s "$JSDIR" spark-job-server
popd > /dev/null

if [ -e /etc/systemd/system/spark-job-server.service ] ; then
  rm /etc/systemd/system/spark-job-server.service
fi
mv "$INSTDIR"/spark-job-server/spark-job-server.service /etc/systemd/system/spark-job-server.service
chmod +x /etc/systemd/system/spark-job-server.service

echo "Starting and enabling and starting spark-job-server"
command -v  systemctl >/dev/null 2>&1 && { systemctl daemon-reload ; systemctl enable spark-job-server.service ; systemctl start spark-job-server.service ; }
# command -v  systemctl >/dev/null 2>&1 || { chkconfig spark-job-server on ; /etc/init.d/spark-job-server start ; }




########### move INITD ##########################################################
#chmod +x $INITD
#mv $INITD $JSDIR/spark-job-server-init.d
#################################################################################
