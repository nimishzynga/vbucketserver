%define _package vbs-1.0.0.0
%define _auto_root /opt/vbucketserver
Summary:      VBS component
Name:         vbs
Version:      1.0.0.0
Release:      1
Group:        Development/Languages
License:      zynga 

BuildRoot:    %{_tmppath}/%{name}-%{version}-root-%(%{__id_u} -n)

Distribution:	Vbs

%description
Vbucketserver component

%build
go get code.google.com/p/goweb/goweb
cd %{_topdir}/%{_package}
go build

%install
%{__mkdir_p} %{buildroot}/%{_auto_root}
%{__install} -m 755 %{_topdir}/%{_package}/%{_package} %{buildroot}/%{_auto_root}/vbucketserver

%clean
%{__rm} -rf %{buildroot}/

%files
%defattr(-, root, root, -)
%{_auto_root}/vbucketserver
#%{_auto_root}/*.sh*

%changelog
* Thu Apr 26 2012 <vdhussa@zynga.com> 1.0.0.0-1.1
 Updates for ANF 2
* Thu Dec 1 2011 <gaagarwal@zynga.com> 1.0.0.0
 Initial version 
