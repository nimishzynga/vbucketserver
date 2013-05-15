#!/bin/sh
FILES="
handlers.go \
main.go    \
server     \
config     \
scripts   \
net
"
vbs_specfile=vbs.spec
vbs_version=`sed -n 's/^Version:[ ]*//p' $vbs_specfile`
package_name="vbucketserver"
topdir=`pwd`/_rpmbuild

rm -rf $topdir 2>/dev/null

mkdir -p $topdir/{SRPMS,RPMS,BUILD,SOURCES,SPECS}
mkdir -p $topdir/src/$package_name

cp -ar $FILES $topdir/src/$package_name && \
cp $vbs_specfile $topdir/SPECS && \
echo "Creating source tgz..." && \
tar -czv --exclude=.svn -f $topdir/SOURCES/$package_name.tgz -C $topdir src/$package_name && \
echo "Building rpm ..." && \
echo "Top dir is: $topdir"
rpmbuild --define="_topdir $topdir" -ba $vbs_specfile && \
cp $topdir/SRPMS/*.rpm . && \
cp $topdir/RPMS/*/*.rpm .
rm -rf $topdir
