The repo:

This repo houses the work we did to monitor Cassandra using Nagios.

How repo is structured:

  src - a patch to apply to nagios so that it does not truncate performance data
  etc - sample nagios commands
  plugins - the check_cassandra.pl plugin and Jolokia.pm wrapper


Why:

Jolokia is a really awesome tool that makes it easy to get at jmx without having
to use jmx or java. If you haven't already you should check out jmx4perl, a
complete version of the Jolokia API, and another very good script
check_jmx4perl.

We decided not to use jmx4perl because it had *too many* features (and
dependencies). We wrote a minimalist Jolokia.pm module, implementing a very
small subset of the Jolokia API (list, read). Also, we wanted to minimize the
number of nagios checks we are making while still exporting all of the
performance datas.


What:

The script, check_cassandra.pl, will query the Jolokia API running inside the
Cassandra JVM, checking an arbitrary number of metrics. The script returns all
of the metrics, not just the checked metrics, as performane data. The examples
directory has an example nagios configuration*.

The script is meant to run under ePn and can check multiple metrics in each run,
so it should be relatively efficient.

The most common check is a regex check (-C regex -r '.*') that will check all
metrics matching the regular expression.

The script can also check the length of a list. The StorageService mbean exposes
lists of nodes in certain states. This is the only place this type of check is
used.

The script can also check for the presence of a string. This is useful, e.g., to
check the cluster is NORMAL and the nodes are UP.

*A disclaimer here: We have not actually started using Cassandra in production so
the thresholds are probably wrong and/or we are not monitoring everything we
should and/or we are monitoring things we probably shouldn't be.


How:

First, you may want to apply the patch in the src directory to Nagios. Nagios
truncates plugin output at an arbitrary buffer size.

Install the required perl modules:

sudo yum install perl-libwww-perl perl-JSON
sudo apt-get install libwww-perl libjson-perl

Configure nagios:

See examples directory.

Collect performance data:

Use graphios. Seriously, use graphios. It *just works*.
