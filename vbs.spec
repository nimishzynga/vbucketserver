%define _package vbucketserver
%define _auto_root /opt/vbucketserver
Summary:      VBS component
Name:         vbs
Version:      1.0.0.0
Release:      12   
Group:        Development/Languages
License:      zynga 

BuildRoot:    %{_tmppath}/%{name}-%{version}-root-%(%{__id_u} -n) 

Distribution:   Vbs 

%description
Vbucketserver component

%build
cd %{_topdir}
export GOPATH=`pwd`
cd %{_topdir}/src/%{_package}
go build

%install
%{__mkdir_p} %{buildroot}/%{_auto_root}
%{__mkdir_p} %{buildroot}/%{_initrddir}
%{__install} -m 755 %{_topdir}/src/%{_package}/%{_package} %{buildroot}/%{_auto_root}/vbucketserver
%{__install} -m 755 %{_topdir}/src/%{_package}/scripts/vbs.sh %{buildroot}/%{_initrddir}/vbucketserver
%{__install} -m 755 %{_topdir}/src/%{_package}/scripts/install_vbs_log_rotation.sh %{buildroot}/%{_auto_root}

%clean
%{__rm} -rf %{buildroot}/

%files
%defattr(-, root, root, -)
%{_auto_root}/vbucketserver
%{_initrddir}/vbucketserver
%{_auto_root}/*.sh*

%post
%{_auto_root}/install_vbs_log_rotation.sh

%changelog
* Mon Jan 21 2013 <nigupta@zynga.com> 1.0.0.0-1.1
Initial version
