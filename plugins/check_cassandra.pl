#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Std;
use lib './';
use Jolokia;

sub usage {
  print qq{ $0 -H <host> -m <mbean> -C <check> -w <warn> -c <crit>
  -h display this message and then exit
  -H <hostname> - the host to check (default: localhost)
  -p <port> - the port to use (default: 7777)
  -m <mbean> - the mbean to query
  -a <attributes> - comma seperated list of attributes to request
  -f <filter> - filter (e.g. type=FooType)
  -T <type(s)> - regular expression matching types to deserialize (default: 'int|long|double|map')
  -t <timeout> - consider a request a failure when the timeout is exceeded (default: 10)
  -C <check> - type of check to perform (e.g. -C regex)
    -r <regex> - required argument to -C regex: check keys matching this regex
    -s <string> - required arguement to -C string: ensure values match this string
  -w <number> - warning threshold
  -c <number> - critical threshold
  -u - perform upper bound checks (i.e. value < crit && value < warn)
  -l - perform lower bound checks (i.e. value > crit && value > warn)
  -v - be verbose
}; # end qq
  exit 1;
};

sub version {
  print "check_cassandra.pl v0.0.1\n";
}

*HELP_MESSAGE = \&usage;
*VERSION_MESSAGE = \&version;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my ($opts, $jolokia_opts) = ({}, {});
getopts('hulvH:p:m:a:f:T:t:C:w:c:r:s:',$opts);

&usage if $opts->{h};
$opts->{C} = 'default' unless defined $opts->{C};
$opts->{w} = 10 unless defined $opts->{w};
$opts->{c} = 20 unless defined $opts->{c};
$opts->{t} = 10 unless defined $opts->{t};
$jolokia_opts->{host} = $opts->{H} if defined $opts->{H};
$jolokia_opts->{port} = $opts->{p} if defined $opts->{p};
$jolokia_opts->{attrs} = $opts->{a} if defined $opts->{a};
$jolokia_opts->{filter} = $opts->{f} if defined $opts->{f};
$jolokia_opts->{type_match} = $opts->{T} if defined $opts->{T};
$opts->{m} = 'internal' unless defined $opts->{m};

sub perf_str {
  my $data = shift;
  my $str = '';
  while ( my ($k, $v) = each %$data ) {
    next unless defined $v;
    next unless $v =~ m/\d+/;
    $k =~ s/org.apache.cassandra.//g;
    $k =~ s/[^a-z0-9\.]//gi;
    $v = sprintf "%.2f", $v;
    $str .= "$k=$v ";
  }
  return $str;
}

sub check_upper {
  my ($key, $value, $opts) = @_;
  my $ret = 0;
  my $msg = '';
  # This should probably return a warning...
  return 0, "[ $key is undefined ]" unless defined $value;
  if ( $value >= $opts->{c} ) {
    $ret = 2;
    $msg = "[ $key is critical $value >= $opts->{c} ]";
  } elsif ( $value >= $opts->{w} ) {
    $ret = 1;
    $msg = "[ $key is warn $value >= $opts->{w} ]";
  } else {
    $msg = "[ $key is OK ]" if $opts->{v};
  }
  return $ret, $msg;
}

sub check_lower {
  my ($key, $value, $opts) = @_;
  my $ret = 0;
  my $msg = '';
  # This should probably return a warning...
  return 0, "[ $key is undefined ]" unless defined $value;
  if ( $value <= $opts->{c} ) {
    $ret = 2;
    $msg = "[ $key is critical $value <=  $opts->{c}]";
  } elsif ( $value <= $opts->{w} ) {
    $ret = 1;
    $msg = "[ $key is warn $value <= $opts->{w} ]";
  } else {
    $msg = "[ $key is OK ]" if $opts->{v};
  }
  return $ret, $msg;
}

sub check_success {
  my ($jo, $opts) = @_;
  my ($ret, $msg) = (0, '');

  if ( $jo->{success} ) {
    if ( $jo->{time} > $opts->{t} ) {
      # timeout
      $ret = 1;
      $msg = "request (GET $jo->{uri}) timed out";
    } else {
      $msg = '[ API OK ]';
    }
  } else {
    # http failure
    $ret = 2;
    $msg = "request (GET $jo->{uri}) failed with $jo->{status})";
  }
  return $ret, $msg;
}

sub init_and_check_success {
  my ($jopts, $opts, $comp) = @_;
  my $jo = Jolokia->new(@$jopts);
  my $jr = $jo->read;
  my ($ret, $msg) = check_success($jr, $opts);
  return $ret, $msg, $jr;
}

# metrics:type=Cache [scope=KeyCache|RowCache]
sub check_cache {
  my ($jopts, $opts, $comp) = @_;
  @$jopts[1]->{filter} = 'type=Cache';
  my ($ret, $msg, $jr) = init_and_check_success(@_);
  my %rates = ();
  my $prefix = "org.apache.cassandra.metrics.Cache";
  foreach my $cache_type ( qw(KeyCache RowCache) ) {
    foreach my $type ( qw(OneMinuteRate FiveMinuteRate FifteenMinuteRate MeanRate) ) {
        my $requests = $jr->{data}->{"$prefix.$cache_type.Requests.$type"};
        my $hits = $jr->{data}->{"$prefix.$cache_type.Hits.$type"};
        $requests = 1 if $requests <= 0;
        my $rate = $hits / $requests * 100;
        my $k = "metrics.Cache.$cache_type.HitRate.$type";
        if ( $rate > 0 ) {
          my ($rret, $mmsg) = &$comp($k, $rate, $opts);
          $ret |= $rret;
          $msg .= $mmsg;
        }
        $rates{$k} = $rate;
      }
  }
  return $ret, $msg, perf_str(\%rates);
}

# metrics:type=ClientRequest [scope=Read|Write|RangeSlice]
sub check_client_request {
  my ($jopts, $opts, $comp) = @_;
  @$jopts[1]->{filter} = 'type=ClientRequest';

  my ($ret, $msg, $jr) = init_and_check_success(@_);
  my %perf;
  while ( my ($k, $v) = each %{$jr->{data}} ) {
    my @results;
    # In the interest of not having too many checks this check has evil
    # hardcoded thresholds. This might be better than having options for
    # thresholds for Read|Write.Latency, RangeSlice.Latency, and Rate(s)?
    if ( $k =~ /(Read|Write).Latency.95/ ) {
      $opts->{w} = 8000;
      $opts->{c} = 12000;
      @results = check_upper($k, $v, $opts);
    } elsif ( $k =~ /RangeSlice.Latency.95/ ) {
      $opts->{w} = 300000;
      $opts->{c} = 600000;
      @results = check_upper($k, $v, $opts);
    } elsif ( $k =~ /Unavailables.*Rate/ ) {
      $opts->{w} = 10;
      $opts->{c} = 20;
       @results = check_upper($k, $v, $opts);
    } elsif ( $k =~ /Timeouts.*Rate/ ){
      $opts->{w} = 10;
      $opts->{c} = 20;
      @results = check_upper($k, $v, $opts);
    } else {
      next;
    }
    $ret |= $results[0];
    $msg .= $results[1];
    $perf{$k} = $v;
  }
  return $ret, $msg, perf_str(\%perf);
}

# other valid filters for 'metrics':
##type=ClientRequestMetrics
##type=ColumnFamily [keyspace=?, scope=?]
##type=CommitLog
##type=Compaction
##type=Connection [scope=?]
##type=DroppedMessage = [scope=BINARY|MUTATION|RANGE_SLICE|READ|READ_REPAIR|REQUEST_RESPONSE|_TRACE]
##type=Storage
##type=ThreadPools [path=internal|request, scope=AntiEntropyStage|FlushWriter|GossipStage|HintedHandoff|InternalResponseStage|MemtablePostFlusher|MigrationStage|MiscStage|MutationStage|ReadRepairStage|ReadStage|ReplicateOnWriteStage|RequestResponseStage|commitlog_archiver]

sub check_regex {
  my ($jopts, $opts, $comp) = @_;
  my ($ret, $msg, $jr) = init_and_check_success(@_);

  while ( my ($k, $v) = each %{$jr->{data}} ) {
    next unless $k =~ m/$opts->{r}/i;
    my ($rret, $mmsg) = &$comp($k, $v, $opts);
    $ret |= $rret;
    $msg .= $mmsg;
  }
  return $ret, $msg, perf_str($jr->{data});
}

sub check_list {
  my ($jopts, $opts, $comp) = @_;
  my ($ret, $msg, $jr) = init_and_check_success(@_);

  while ( my ($k, $v) = each %{$jr->{data}} ) {
    my ($rret, $mmsg) = &$comp($k, scalar @$v, $opts);
    $ret |= $rret;
    $msg .= $mmsg;
  }
  return $ret, $msg, '';
}

sub check_string {
  my ($jopts, $opts, $comp) = @_;
  my ($ret, $msg, $jr) = init_and_check_success(@_);
  while ( my ($k, $v) = each %{$jr->{data}} ) {
    if ( $v ne $opts->{s} ) {
      $ret = 2;
      $msg .= "[ $k is not $opts->{s} ($v) ]";
    }
  }
  return $ret, $msg, '';
}

if ( $opts->{m} eq 'metrics' ) {
  if ( $jolokia_opts->{filter} ) {
    if ( $jolokia_opts->{filter} =~ m/type=ColumnFamily/i ) {
      $jolokia_opts->{formatter} = [qw(type keyspace scope name)];
    } elsif ( $jolokia_opts->{filter} =~ m/(path|type=threadpools)/i ) {
      $jolokia_opts->{formatter} = [qw(type path scope name)];
    } else {
      $jolokia_opts->{formatter} = [qw(type scope name)];
    }
  } else {
    $jolokia_opts->{formatter} = [qw(type scope name)];
  }
} elsif ( $opts->{m} eq 'net' ) {
    $jolokia_opts->{formatter} = [qw(type)];
} elsif ( $opts->{m} eq 'internal' ) {
    $jolokia_opts->{formatter} = [qw(type)];
} elsif ( $opts->{m} eq 'request' ) {
    $jolokia_opts->{formatter} = [qw(type)];
} elsif ( $opts->{m} eq 'db' ) {
  if ( $jolokia_opts->{filter} ) {
    if ( $jolokia_opts->{filter} =~ m/type=ColumnFamilies/i ) {
      $jolokia_opts->{formatter} = [qw(type keyspace columnfamily)];
    } else {
      $jolokia_opts->{formatter} = [qw(type)];
    }
  } else {
    $jolokia_opts->{formatter} = [qw(type)];
  }
} else {
  print "unknown cassandra mbean\n";
  exit 1
}

my $mbean_prefix = 'org.apache.cassandra';
my $jopts = ["$mbean_prefix.$opts->{m}", $jolokia_opts];
my $handler;;
my $comp = \&check_lower;
$comp = \&check_upper if $opts->{u};

if ( $opts->{C} eq 'regex' ) {
  die 'must specify -r <regex>' unless defined $opts->{r};
  $handler = \&check_regex;
} elsif ( $opts->{C} eq 'cache' ) {
  $handler = \&check_cache;
} elsif ( $opts->{C} eq 'client_request' ) {
  $handler = \&check_client_request;
} elsif ( $opts->{C} eq 'list' ) {
  $handler = \&check_list;
} elsif ( $opts->{C} eq 'string' ) {
  die 'must specify -s <string>' unless defined $opts->{s};
  $handler = \&check_string;
} else {
  die 'must specify valid check: -C <check>';
}

my ($ret, $msg, $perf) = &$handler($jopts, $opts, $comp);

print $msg, '|', $perf, "\n";
$ret = 2 if $ret > 2;
exit $ret;
