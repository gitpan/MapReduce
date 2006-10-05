package MapReduce;

use 5.006;
use strict;
use warnings;

use FileHandle;
use Digest::MD5 qw{ md5 };

our $VERSION = '0.01';


sub run {
  my $package = bless {}, shift;
  my $task = shift;
  die "Unknown task" unless ($task eq 'map' 
                          or $task eq 'reduce' );

  $task = \&maptask    if $task eq 'map';
  $task = \&reducetask if $task eq 'reduce';
  
  $task->($package, @_);
  exit;
}


sub maptask {

  my ( $package, $repository, $corpus, $result, $k, @hosts ) = @_;

  prepare_resultdir( $repository, $result );

  # Determine the list of input files.
  my @in =
    grep { m|/[^/]+$| } glob("$repository/$corpus/*");

  # Pass the files to the reader, and pass the resulting records to
  # the user-written Map function. Periodically write those results
  # to disk.
  my @results = ();
  my $bytes = 0;
  my ( $partitioner, $closer ) =
    partition_writer( $package, "$repository/$result/map-out", @hosts );

  while (@in) {
    my $in     = shift @in;
    $bytes += -s $in;
    my $openstring = ($in =~ /gz$/) ? "gunzip $in -c |" : "<$in";
    my $fh     = new FileHandle $openstring or die $!;
    my $reader = $package->can("Reader");
    $reader = $reader->($fh);
    my $mapper = $package->can("Map");
    while ( my $record = $reader->() ) {
      push @results, $mapper->( $record );
      if ( @results >= 10000 ) {
        $partitioner->( \@results );
        @results = ();
      }
    }
  }
  $partitioner->( \@results );

  print STDERR "$hosts[$k] finished map tasks ($bytes bytes input). Distributing partitions\n";
  
  # Take all output files and distribute, according to partition, into
  # their proper hosts' job directory
  my @outfiles = $closer->();    # close the filehandles
  for (@outfiles) {
    my ($partition) = (m|(\d+)$|);
    my $properhost  = $hosts[$partition];
    my $reducefile  = $hosts[$k] . '-' . $partition;
    if( $partition == $k ){
      print STDERR "Moving file $_ locally to $reducefile\n";
      system "gunzip -c $_ > $repository/$result/reduce-in/$reducefile";
    }
    else {
      print STDERR "copying file $_ to $properhost as $reducefile\n";
      system "cat $_ | ssh $properhost \"gunzip -c > $repository/$result/reduce-in/$reducefile\"";
    }
    unlink $_ or die $!;
  }

  rmdir "$repository/$result/map-out";

  print STDERR "$hosts[$k] finished distributing partitions\n";
}



# Takes a stream and returns an iterator for key/value pairs.
# Default is for documents with DOCNO and TEXT sections.
#sub Reader {
#  my $fh = shift;
#
#  local $/ = "</DOC>";
#
#  sub {
#    my $record = <$fh>;
#    return undef unless defined $record;
#    my ( $key )   = ( $record =~ /<DOCNO>(.+?)<\/DOCNO>/ );
#    my ( $value ) = ( $record =~ /<TEXT>(.+?)<\/TEXT>/ );
#    return \$value;
#  }
#}


sub partition_writer {
  my $package = shift;
  my $partsdir = shift;
  my @hosts = @_;
  my $R = @hosts;
  my @table = ();

  my $partition_func;
  unless( $partition_func = $package->can("Partition") ){
    $partition_func = \&hash;
  }
  
  # writes to the proper partition filehandle, creating it if necessary
  sub {
    my $mapped = shift;

    for my $i (0..(@$mapped/2)-1){
      $i *= 2;
      my $section = $partition_func->($mapped->[$i]) % $R;
      $table[$section] ||= new FileHandle "| gzip -c --fast > $partsdir/$section"
        or die $!;

      $table[$section]->print( 
        $mapped->[$i], "\t", $mapped->[$i+1], "\n");
    }
  },

  # closes up the filehandles, returning the list of files present
  sub {
    my @opened = 
      map  { "$partsdir/$_" }
      grep { defined $table[$_] && $table[$_]->close } 
      0..$#table;
    return @opened;
  }
}


sub hash_old {
  my $hash = 0;
  for ( split //, shift ) {
    $hash = $hash * 37 + ord($_);
  }
  return $hash;
}

sub hash {
  return undef unless defined $_[0];
  return unpack("L", md5( shift ) );
}


sub reducetask {
  my ($package, $repository, $corpus, $result, $k, @hosts) = @_;

  my @mapped = glob "$repository/$result/reduce-in/*";
  my $reduce = $package->can("Reduce");

  # If Reduce() is undefined, simply treat reduce-in as final.
  unless ($reduce) {
    for (@mapped) {
      print STDERR "zipping $_\n";
      my ($file) = (m|/([^/]+)$|);
      system "gzip --fast $_";
      rename "$_.gz", "$repository/$result/$file.gz";
    }

    rmdir "$repository/$result/reduce-in";
    print STDERR "zipped and renamed ", scalar @mapped, " files\n";
    return;
  }

  # Determine the list of input files.
  my @in = grep { m|/[^/]+$| } @mapped;

  my $sorted = "$repository/$result/reduce-in/sorted";
  my $sort_cmd = "sort -k1,1 -k2,2 -o $sorted " . join " ", @in;
  print STDERR "$hosts[$k] beginning sort\n";
  my $start = time;
  die "Sort error" if system $sort_cmd;
  print STDERR "$hosts[$k] sort done in ", time - $start," seconds\n";

  # remove unsorted input files
  unlink @in;

  my $fh = new FileHandle "<$sorted" or die $!;
  my $outfd = 
    new FileHandle "| gzip -c > $repository/$result/$result.gz" 
      or die $_;

  my $reducer_itergen = reduce_iterator( $fh );
  while(my ($key, $iterator) = $reducer_itergen->()){
    last unless defined $iterator;
    my @row = $reduce->($key, $iterator);
    next unless defined $row[0];
    print $outfd join("\t", @row), "\n";
  }

  unlink $sorted;
  rmdir "$repository/$result/reduce-in"
}


# An iterator generator. I have gone quite mad.
sub reduce_iterator {
  my $fh = shift;
  
  my $key  = '';
  my @vals = ();
  my $line = <$fh>; # cache first line
  chomp $line;
  
  sub {

    return undef unless defined $key;

    return $key,
    sub {
      unless(defined $line){
        undef $key;
        return undef;
      }

      my ($newkey, @vals) = split /\t/, $line;
      if($newkey ne $key){
        $key = $newkey;
        return undef;
      }
      $line = <$fh>; # cache new line
      chomp $line if defined $line;

      return \@vals;
    };
  };		
}

sub prepare_resultdir {
  my ($repository, $result) = @_;
  mkdir "$repository/$result";
  mkdir "$repository/$result/map-out";
  mkdir "$repository/$result/reduce-in";
}





## Make an anonymous variable reference.
## This is necessary to abide by the Getopt::Long way.
#sub ar {
#  my $var = shift;
#  return \$var;
#}
#
## Get all configuration info, with precedence given to cmdline
#sub getconf {
#  my %opts = @_;  
#
#  GetOptions( %opts );
#  
#  my %cmdline = 
#    map { (split '=', $_)[0] => [ ${$opts{$_}} ] } 
#    grep { ${$opts{$_}} } 
#    keys %opts;
#  
#  # Read in config file and overrwrite with cmd line options
#  my %conf = readconf( @{$cmdline{conf}} );
#  $conf{$_} = $cmdline{$_} for keys %cmdline;
#
#  return %conf;
#}
#
#
#sub readconf {
#  my $fname = shift;
#  return () unless -e $fname; 
#  my @c     = ();
#  open F, "<$fname";
#  while (<F>) {
#    chomp;
#    s/\r//;
#    next unless $_;
#    next if /^#/;
#    push @c, [ split /\t+/ ];
#  }
#  my %conf = ();
#  for my $list (@c) {
#    if ( @$list > 2 ) {    # this is a vector
#      push @{ $conf{ shift @$list } }, [@$list];
#    }
#    else {                 # this is a single key/value
#      push @{ $conf{ $list->[0] } }, $list->[1];
#    }
#  }
#  return %conf;
#}













1;
__END__

=head1 NAME

MapReduce.pm - Perl version of Google's distributed data processing

=head1 ABSTRACT

Simple, generalized distributed data processing.


=head1 SYNOPSIS

  On the commandline:
  # mr example.mr inputcorpus outputcorpus

  File example.mr:
  ------------------
  package ExamplePackage;

  use MapReduce;
  @ISA = qw( MapReduce );

  ExamplePackage->run;

  sub Reader { ... }
  sub Map { ... }
  sub Reduce { ... }
  ------------------


=head1 DESCRIPTION


MapReduce is a generalized distributed data processing system (see http://labs.google.com/papers/mapreduce.html). You are B<strongly> encouraged to read the paper before proceeding further, as it will make everything much clearer.

There is at least one open implementation of mapreduce. The purpose of MapReduce.pm is to be 1. Perl-based, using Perl as the default language for mapreduce programs, and 2. as simple as possible while still being useful. Some people, like me, have a particular thing for simplicity.

This is a simplified, easy to use implementation. Based on shared ssh keys and remote calls, it is currently suitable for small clusters (<=100 or so machines).

To use it, you just write three functions into a .mr file: Reader(), Map(), and Reduce(). Reader defines an input record and returns an iterator for them. Map takes an input record and creates a list of key/value pairs from it. Reduce takes a key and a list of values, and returns a (possibly different) key and another list (possibly a single value).

Strange as it may seem, an astonishingly large class of data processing tasks can be formulated as definitions of these three functions.


For example, here is a .mr file which counts all the occurrences of every unique word in a corpus of documents:

 #### file MapReduce/bin/wc.mr
 #!perl -w
 use strict;

 package WordCount;

 use lib "MapReduce/lib";
 use MapReduce;
 our @ISA = qw( MapReduce );

 WordCount->run(@ARGV);

 ###########


 # Reads one line in per record
 sub Reader {
   my $fh = shift;
 
   sub {
     my $record = <$fh>;
     return () unless defined $record;
     return \$record;
   }
 }


 # returns a flat list with key/value pairs eg 
 # (word => 1, word2 => 1, ...)
 sub Map {
   my $ref = shift;
   return map { $_ => 1 } split /\W+/, $$ref;
 }


 # returns a single key/value pair, reduced from an iterator of values.
 sub Reduce {
   my ($key, $iter) = @_;
   my $val;
   while( defined (my $next = $iter->()) ){
     $val += $next->[0];
   }
   return $key, $val;
 }
 #####################################

When this is called using the B<mr> program, like so

 # mr wc.mr odyssey odyssey_wc

...there will appear a new corpus called odyssey_wc, containing records of the form "word sum\n" where sum is the number of occurrences of that word.


 




=head1 HOWTO

How to gather machines into a cluster.

1. Procure one or more machines. Designate one machine as "master". This machine might participate in the distributed tasks, but not necessarily. A single machine will work as well.

2. Place a correct config file on the master machine as /etc/mapreduce.conf, with entries for each host in the pool. See mapreduce-sample.conf for a template.
  
3. On each machine, create a uniform username for the mapreduce user (this can be any username). The command "useradd mapreduce -m -G users -s /bin/bash" will do this.
  
4. Log in to the mapreduce user account on the master machine. Install the mr program into the ~/bin dir and make sure this is in your path. Generate and distribute shared ssh keys for the mapreduce user accounts. This is a bit involved, so it has its own section below.

5. On each machine, create a uniformly named, writable directory. Default is /tmp/mr. It's nice to mount a whole disk on this, if you have extras. This could also be a symbolic link to somewhere there is a lot of space. The minimal-effort thing to do is 

  # mr -cmd "mkdir /tmp/mr"

6. Place a MapReduce.pm install dir in the home directory of each mapreduce user, and rename it so there is no version number. This can be achieved like this from the master account (after the above steps): 

  # mr -cmd="scp masterhost:MapReduce-0.01.tar.gz ."
  # mr -cmd="tar zxvf MapReduce*"
  # mr -cmd="mv MapReduce-0.01 MapReduce"


Your system is ready to go. When adding an additional machine, just remember all hosts must share the same ssh keypair, and they all have to have entries for everyone in their .ssh/known_hosts file. Therefore repeat steps 1 and 2 for the new host, and redistribute the master's .ssh/known_hosts file to the pool after ssh'ing to the new host. And don't forget to add the new host to the /etc/mapreduce.conf!


 
=head2 GENERATING SHARED SSH KEYS FOR THE MACHINE POOL

All machines in the pool need shared ssh keys. There are three steps to this.

 - Generate a key pair.

 - distribute the key pair across all hosts.

 - distribute the host list across all hosts.

You can use the following procedure:

Generate the key pair.

 ssh-keygen -t rsa

(hit enter whenever it asks you for any information, B<including passphrase>)

 cp .ssh/id_rsa.pub .ssh/authorized_keys2


Distribute the keys.

 # mr -cmd="mkdir .ssh; chmod 0700 .ssh"
 # mr -cmd="scp masterhost:.ssh/* .ssh/."

Distribute the host list.

 # mr -cmd="scp masterhost:.ssh/known_hosts .ssh/."

NOTE: Please see Hack #66 in the excellent B<Linux Server Hacks> by Rob Flickenger for details beyond the above instructions.
 

=head1 USING MAPREDUCE

The main tool is the B<mr> program. You can use this to launch mapreduce operations or just execute any command on every host in the pool.

After all the setup has been done, there should be sufficient information in the config file and command line for mapreduce to know what to do. This info includes
 - the list of hosts in the pool
 - the location of the repository (default /tmp/mr)
 - the name of the task file where your program resides
 - the name of the input corpus 
 - the name of the result corpus

Normally only the last three are specified on the command line. Hence the typical call looks like this:

  # mr wc.mr odyssey odyssey_wc

If something goes wrong and you need to clean up, this will get rid of the distributed output corpus:

  # mr -cmd="rm -r /tmp/mr/odyssey_wc"

(this assumes your repository is /tmp/mr and corpus to delete is odyssey_wc).

The B<cmd> option is quite useful in practice, but be careful!




=head1 REQUIRES

Ssh using shared keys.

Digest::MD5



=head1 DIFFERENCES FROM OTHER IMPLEMENTATIONS

There are two implementations of this model (that I know of): Google's original, and a Lucene-related project called B<Hadoop>.

Compared to Google's much more involved version, this is more or less a toy. It's only suitable for smaller clusters, for various reasons.

There is no fault tolerance, so it's only useful where machine failure is uncommon. 

It's based not on listening services per host, but instead on remote command invocation from a master program which forks processes to babysit the tasks. This makes the code rather simple and requires no true installation on any host in the pool. On the other hand it limits the number of hosts to available process memory on the master machine. Plus any network trouble, or machine failure, is going to mess everything up.

A quite significant difference is the absense of a distributed file system. Google uses "GFS" to make input files available to map tasks, while this implementation simply stores distributed files across the cluster so that each host is responsible for the files on its local storage. This requires a preliminary "distribution step" which partitions a set of records across all hosts. This is quite simple since it can be implemented as a MapReduce operation! See 'partition_trec_docs.mr' and 'partition_lines.mr' in the bin directory for examples. This turns out to be more convenient than it looks, because often you will want to perform another mapreduce operation on the results of the last, and in this way the files are already distributed.

I haven't looked at Hadoop very much. Only to note that it appears much more "enterprise", which is to say it has four xml configuration files (MapReduce.pm has only one, on one machine), and ... a lot of source code. It's no doubt much more stable, and also harder to get started. Not to mention... there are no Perl bindings.

One final note on relative complexity. The current Hadoop build has 313 java files with 61,810 lines of code. This doesn't include non-java code. There are 14 jsp files, 13 sh files, and 586 html files... anyway you get the picture. You could not check out the source code and understand everything in half a day. I'm not disparaging the Hadoop effort at all. It's probably very high quality. I'm just saying the concept is very simple, and it might be possible to have a simple yet stable implementation.

Even though this version is probably not as stable as the others, it comprises barely 400 lines of code as of the first release. It has only one configuration file, can be used almost immediately with very minimal effort, and is rather noninvasive, requiring nothing but untar-ing a directory on pool machines.

In short, perfect for exploring, understanding, and profitably using the MapReduce concepts with near-trivial time investment.


=head1 TO DO

It's my goal to have a very sound system with comparable performance and stability to Google or Hadoop, using so little code that it can be read and understood in less than a day by a normal programmer.

The main glaring holes are B<fault tolerance> and B<load balancing>. Currently the files are distributed evenly across hosts so that the last host to finish holds up all the rest, and effectively you are bound by the speed of the slowest. Load balancing would take care of that. Fault tolerance is even more important, since the entire operation can crash with one network hiccup or machine failure.

Google and Hadoop deal with these issues separately, using a distributed file system and centralized tracking of tasks. I don't particularly like those because they're complicated and would require a lot more code than I feel is really necessary. I'm dreaming up something along the lines of "replicated responsibility" to handle both load balancing and fault tolerance. I'll write more about it when it gets clearer to me... these are early days yet.

Please, if you have suggestions for elegant improvements, get in touch with me.



=head1 SEE ALSO

http://labs.google.com/papers/mapreduce.html

Linux Server Hacks, Rob Flickenger


=head1 AUTHOR

Ira Woodhead, E<lt>ira at sweetpota dot to<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by Ira Woodhead

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.6 or,
at your option, any later version of Perl 5 you may have available.


=cut

