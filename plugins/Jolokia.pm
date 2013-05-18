#!/usr/bin/perl
package Jolokia;
use strict;
use warnings;
use Digest::MD5 qw(md5_hex);
use Getopt::Std;
use HTTP::Cookies;
use HTTP::Headers;
use JSON;
use LWP::UserAgent;
use Storable;

sub new {
  my $class = shift;
  my ($mbean, $opts) = @_;

  $opts->{host} = 'localhost' unless defined $opts->{host};
  $opts->{port} = 7777 unless defined $opts->{port};

  my $self = {
    _ua => LWP::UserAgent->new(
      requests_redirectable => [ 'GET', 'HEAD', 'POST']
    ),
    qualified_mbean => $mbean,
    base_uri => "http://$opts->{host}:$opts->{port}/jolokia/",
    _json => JSON->new->allow_nonref,
    json => {},
    attrs => [],
    filter => ['*'],
    formatter => $opts->{formatter},
    type_match => 'int|long|double|map',
  };
  push @{$self->{filter}}, $opts->{filter} if $opts->{filter};
  push @{$self->{attrs}}, $opts->{attrs} if $opts->{attrs};
  $self->{type_match} = $opts->{type_match} if $opts->{type_match};

  bless $self, $class;
  $self->init;
  return $self;
}

sub init {
  my $self = shift;
  $self->{_ua}->cookie_jar(
    HTTP::Cookies->new(
      file => "/tmp/nagios/$self->{qualified_mbean}.dat",
      autosave => 1,
    )
  );
  $self->{_ua}->agent("Mozilla/5.0 (X11; U; SunOS sun4u; en-US; rv:1.0.1) Gecko/20020920 Netscape/7.0");
}

sub json {
  my $self = shift;
  return $self->{json};
}

sub request {
  my $self = shift;
  my ($uri) = @_;
  my $ret = {};
  my $req = HTTP::Request->new('GET', $uri) || die $@;
  $req->header('Connection' => 'close');
  my $t1 = time;
  my $res = $self->{_ua}->request($req);
  my $t2 = time;
  $self->{diff} = $t2 - $t1;
  $self->{_response} = $res;
  if ( $res->is_success ) {
    $ret = $self->{_json}->decode($res->content);
    $self->{_response}->code($ret->{status});
  }
  return $ret;
}

sub get_or_set {
  my $self = shift;
  my ($args) = @_;

  my $tmp_path = "/tmp/".md5_hex(join(',', $args));
  my $json = {};

  if ( -e $tmp_path && ((stat($tmp_path))[10] - time) < 86400*5 ) {
    $json = retrieve($tmp_path);
  } else {
    $json = $self->request($args);
    if ( 0 < scalar keys %$json ) {
      store($json, $tmp_path);
    }
  }
  return $json;
}

sub _list {
  my $self = shift;
  my $uri = $self->{base_uri}.
    "list/".
    $self->{qualified_mbean};
  $self->{uri} = $uri;
  my $json = $self->get_or_set($uri);
  $self->{type_info} = $json->{value} if $json->{value};
}

sub _key_prefix {
  my $self = shift;
  my ($mbean, $pairs) = @_;
  my $data = {};
  my @usekeys;
  foreach my $pair ( split(/,/, $pairs) ) {
    my ($k, $v) = split(/=/, $pair);
    $data->{$k} = $v;
  }
  foreach ( @{$self->{formatter}} ) {
    push @usekeys, $_ if defined $data->{$_};
  }
  return "$mbean.".join('.', map { $data->{$_} } @usekeys);
}

sub _read {
  my $self = shift;
  my $ret = {};

  $self->_list;
  return unless $self->{type_info};

  my $filter = join(',', @{$self->{filter}});
  my $uri = $self->{base_uri}.
    "read/".
    $self->{qualified_mbean}.
    ":$filter";

  $uri = $uri."/".join(',', @{$self->{attrs}}) if scalar @{$self->{attrs}} > 0;

  $self->{uri} = $uri;

  my $json = $self->request($uri);
  while ( my ($type, $type_config) = each %{$json->{value}} ) {
    my ($mbean, $type) = split(/:/, $type);
    my $key_prefix = $self->_key_prefix($mbean, $type);
    while ( my ($attr, $attr_value) = each %$type_config ) {
      my $t = $self->{type_info}->{$type}->{attr}->{$attr}->{type};
      next unless $t;
      if ( grep m/$self->{type_match}/i, $t ) {
        if ( grep m/map/i, $t ) {
          while ( my ($mkey, $mval) = each %$attr_value ) {
            $ret->{"$key_prefix.$attr.$mkey"} = $mval;
          }
        } else {
          $ret->{"$key_prefix.$attr"} = $attr_value;
        }
      }
    }
  }
  return $ret;
}

sub read {
  my $self = shift;
  my $data = $self->_read(@_);
  return {
    data => $data,
    time => $self->{diff},
    status => $self->{_response}->code,
    uri => $self->{uri},
    msg  => $self->{_response}->message,
    success => $self->{_response}->is_success,
  }
}
1;
