package MapReduce;

use 5.006;
use strict;
use warnings;

use FileHandle;
use Digest::MD5 qw{ md5 };

our $VERSION = '0.03';


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

sub DefaultReader {
  my $fh = shift;
 
  my $record = <$fh>;
  return () unless defined $record;
  return \$record;
}


sub maptask {

  my ( $package, $master, $repository, 
       $corpus, $result, $k, $fway, $procs, $R, @hosts ) 
    = @_;

  prepare_resultdir( $repository, $result );
  my $status = sub { shift_status( $master, @_ ) };
  my $reader = $package->can("Reader") || \&DefaultReader;

  # spawn children
  for my $child ( 1 .. $procs ) {
    next if fork;

    # Determine the list of input files.
    my @in = glob("$repository/$corpus/*");

    # Pass the files to the reader, and pass the resulting records to
    # the user-written Map function. Periodically write those results
    # to disk.
    my $bytes = 0;
    my ( $partitioner, $closer ) =
      partition_writer( $package, "$repository/$result/map-out", 
                        $k, $fway, $R, @hosts );

    my $statusdir = "$repository/$result/.status/map";
    while (@in) {
      my $infile = shift @in;
      my ($name) = ( $infile =~ m|/([^/]+)$| );
      my $unprocessed = "$statusdir/$name.unprocessed";
      my $inprogress  = "$statusdir/$name.inprogress-$hosts[$k]";
      my $done        = "$statusdir/$name.done-$hosts[$k]";

      # atomically rename status file; continue if rename fails
      next if $status->( $unprocessed => $inprogress );

      my $openstring = 
        ( $infile =~ /gz$/ ) ? "gunzip $infile -c |" : "<$infile";
      my $fh = new FileHandle $openstring or die $!;
      my $read_iter = read_iterator( $reader,  $fh );
      my $mapper = $package->can( "Map" );
      my @results = ();
      while ( my $record = $read_iter->() ) {
        $bytes += length $record;
        push @results, $mapper->( \$record );
        if ( @results >= 10000 ) {
          $partitioner->( \@results );
          @results = ();
        }
      }
      $fh->close;
      $partitioner->( \@results );
      $status->( $inprogress => $done );
    }
    $closer->();    # close the write filehandles
    print STDERR
     "$hosts[$k] ($child) finished map tasks ($bytes bytes input)\n";

    exit;           # child exits
  }

  # wait for children
  while ( wait != -1 ) { }

  print STDERR 
    "$hosts[$k] finished map tasks. Distributing partitions\n";

  distribute_files( "$repository/$result/map-out", 
                    "$repository/$result/reduce-in", 
                     $k, @hosts );

  unlink glob "$repository/$result/map-out/*" 
    or warn "no files mapped by $hosts[$k]\n";
  rmdir "$repository/$result/map-out" or die $!;

  print STDERR "$hosts[$k] finished distributing partitions\n";
}

# Once an operation is done, replicate the resulting files to the other
# machines in the pool, according to the seed which is part of the 
# filename.
sub distribute_files {
  my ( $source, $dest, $k, @hosts ) = @_;

  # Take all files and distribute, according to seed value, into
  # their proper hosts' directory
  my @files = glob "$source/*";
  for (@files) {
    my ($file) = (m|/([^/]+)$|);
    next unless 
      my ($seed, $fway) = ($file =~ m|\.([^\.]+).mr(\d+)(?:\.gz)?$|);

    # replicate reducefiles to F responsible hosts
    my @ihosts = select_nonrepeating( $seed, $fway, $#hosts );
    print STDERR "$file $hosts[$k] -> ", join(", ", @hosts[@ihosts]), " (via seed $seed, fway $fway, hosti $#hosts yields ", join(" ", @ihosts), ")\n";
    for my $i (@ihosts){
      if( $i == $k ){
        system "cp $source/$file $dest/$file";
      }
      else {
        my $ret = system "scp $source/$file $hosts[$i]:$dest/$file";
        warn "Problem with $hosts[$i]: could not copy to $dest/$file"
          if $ret;
      }
    }
  }
  return scalar @files;
}


# Notify other hosts with responsibility for this file that the 
# status of processing has changed. Provide return code to indicate
# success or failure.
sub shift_status {
  my ( $master, $old, $current ) = @_;
  return system "ssh $master \'mv $old $current 2> /dev/null\'";
}


# using random $seed generate $n nonrepeating numbers in the range 0-$range
sub select_nonrepeating {
  my( $seed, $n, $range ) = @_;
  $n = $range+1 if ($n > $range+1);
  my %seen = ();
  my @list = ();

  srand( hashcode($seed) );
  while( $n > 0 ){
    my $k = int rand($range + 1);
    next if $seen{$k};
    $seen{$k} = 1;
    push @list, $k;
    $n--;
  }
  return @list;
}



sub partition_writer {
  my ($package, $partsdir, $k, $fway, $R, @hosts) = @_;
  my @table = ();

  my $partition_func;
  unless( $partition_func = $package->can("Partition") ){
    $partition_func = \&hashcode;
  }
  
  # writes to the proper partition filehandle, creating it if necessary
  sub {
    my $mapped = shift;

    for my $i (0..(@$mapped/2)-1){
      $i *= 2;
      my $section = $partition_func->($mapped->[$i]) % $R;
      $table[$section] ||= 
        new FileHandle ">>$partsdir/$hosts[$k].$section.mr$fway" or die $!;

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


sub hashcode {
  return undef unless defined $_[0];
  return unpack("N", md5 shift );
}


sub reducetask {
  my ($package, $master, $repository, 
      $corpus, $result, $k, $fway, $procs, $R, @hosts) = @_;

  my @mapped = glob "$repository/$result/reduce-in/*";
  my $status = sub { shift_status( $master, @_ ) };
  
  # Determine the list of partitions
  my %parts = map { m|(\d+)\.mr\d+$|; $1 => 1 } @mapped;
  my $statusdir = "$repository/$result/.status/reduce";

  # If Reduce() is undefined, simply treat reduce-in as final.
  my $reduce = $package->can("Reduce");
 
  for my $part (keys %parts){
    my $unprocessed = "$statusdir/$part.unprocessed";
    my $inprogress  = "$statusdir/$part.inprogress";
    my $done        = "$statusdir/$part.done-$hosts[$k]";

    next if $status->( $unprocessed => $inprogress );

    # mother process leaves the rest of loop to child(ren)
    if( fork ){
      $procs--;
      wait if $procs <= 0;
      next;
    }

    my @in = grep { m|\.$part\.mr\d+$| } @mapped;

    # if Reduce() is defined, make the only input file a sorted
    # result of all files for this partition, and set the result
    # iterator to be the reducer. Otherwise, refrain from sorting
    # the partitions, use them all as input to the reducer, and use 
    # Reader() as the reducer.
    my $result_iterator;
    if($reduce){
      my $sorted = "$repository/$result/reduce-in/$part-sorted";
      my $sort_cmd = "sort -k1,1 -k2,2 -o $sorted " . join " ", @in;
      print STDERR "$hosts[$k] beginning sort of $part th partition\n";
      my $start = time;
      die "Sort error" if system $sort_cmd;
      print STDERR 
        "$hosts[$k] sort of $part done in ", time - $start," seconds\n";

      # remove unsorted input files
      unlink @in;
      @in = ($sorted);
      my $infd = new FileHandle "<$sorted" or die $!;
      $result_iterator = reduce_iterator( $reduce, $infd );
    } else {
      my $in = join " ", @in;
      my $infd = new FileHandle "cat $in |" or die $!;
      $result_iterator = 
        read_iterator( $package->can("Reader") || \&DefaultReader, $infd );
    }


    my $md5 = Digest::MD5->new;
    my($outfd, $bytes);

    # begin a new output file
    my $startoutfile = sub {
      $outfd = 
        new FileHandle "| gzip -c > $repository/$result/reduce-out/$part.gz" 
        or die $_;
      $bytes = 0;
    };

    # do this when enough has been output
    my $endoutfile = sub {
      $outfd->close;
      my $chksum = $md5->hexdigest; # (resets $md5 to blank context btw)
      rename "$repository/$result/reduce-out/$part.gz",
             "$repository/$result/reduce-out/in.$chksum.mr$fway.gz";
    };

    $startoutfile->();
    while(my @row = $result_iterator->()){
      last unless defined $row[0];
      my $to_print = join("\t", @row) . "\n";
      $bytes += length $to_print;
      $md5->add($to_print);
      print $outfd $to_print;
      if($bytes >= 2**27){
        $endoutfile->();
        $startoutfile->();
      }
    }
    $endoutfile->();

    unlink @in;

    $status->( $inprogress => $done );
    exit;
  }

  # wait for any children
  while( wait != -1 ){}

  unlink glob "$repository/$result/reduce-in/*";
  rmdir "$repository/$result/reduce-in" or die $!;

  print STDERR 
    "$hosts[$k] finished reduce tasks. Distributing results\n";

  distribute_files( "$repository/$result/reduce-out", 
                    "$repository/$result", 
                     $k, @hosts );

  unlink glob "$repository/$result/reduce-out/*" 
    or warn "$hosts[$k] had no reduce files to distribute\n";
  rmdir "$repository/$result/reduce-out" or die $!;

  print STDERR "$hosts[$k] finished distributing results\n";
}


# Takes a file descriptor and reader function.
# Returns an iterator for records.
sub read_iterator {
  my( $reader, $fh ) = @_;
  
  sub {
    my $ref = $reader->($fh);
    return defined $ref ? ${ $ref } : undef;
  };
}

# Takes a file descriptor and reduce function. 
# Returns an iterator for reduce results
sub reduce_iterator {
  my( $reducer, $fh ) = @_;
  
  # advance to first line w/tab, cache that line
  my $line;
  while($line = <$fh>){ last if $line =~ /\t/ }; 
  chomp $line;
  my ($key, @vals) = split /\t/, $line;

  # val_iterator returns single record on each kick.
  # Pass this to the reducer.
  my $val_iterator = 
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
      $line = <$fh>; # cache new line in prep for next kick
      chomp $line if defined $line;

      return \@vals;
    };

  # The returned iterator provides reduce results on each kick.
  return 
    sub {
      return undef unless defined $line;
      return $reducer->( $key, $val_iterator );
    };
}

sub prepare_resultdir {
  my ($repository, $result) = @_;
  mkdir "$repository/$result";
  mkdir "$repository/$result/map-out";
  mkdir "$repository/$result/reduce-in";
  mkdir "$repository/$result/reduce-out";
}



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

To use it, you just write three functions into a .mr file: Reader(), Map(), and Reduce(). Reader defines an input record, returning one per call. Map takes an input record and creates a list of key/value pairs from it. Reduce takes a key and a list of values, and returns a (possibly different) key and another list (possibly a single value).

Strange as it may seem, an astonishingly large class of data processing tasks can be formulated as definitions of these three functions.


For example, here is a .mr file which counts all the occurrences of every unique word in a corpus of documents:

 #### file MapReduce/bin/wc.mr
 
 package WordCount;

 use lib "MapReduce/lib";
 use MapReduce;
 our @ISA = qw( MapReduce );

 WordCount->run(@ARGV);

 # Map() returns a flat list with key/value pairs eg 
 # (word => 1, word2 => 1, ...)
 sub Map {
   my $ref = shift;
   return map { $_ => 1 } split /\W+/, $$ref;
 }


 # Reduce() returns a single key/value pair, reduced from an iterator 
 # of values.
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


 




=head1 INSTALL

Installation differs from the normal procedure. First you must get the ssh transport layer ready at least for the one host.

=head2 generate an ssh key pair

Call "ssh-keygen" as below, hitting return for all interactive questions.

  ssh-keygen -t rsa
  cat .ssh/id_rsa.pub >> .ssh/authorized_keys2
  ssh localhost
  exit

These steps prepare the ssh transport layer for a single machine.
  
=head2 "make test" for a single-machine cluster

You must rename the B<MapReduce-[version]/> dir to plain B<MapReduce>, and B<there is no make install step>.

  tar zxvf MapReduce-0.03.tar.gz
  mv MapReduce-0.03 MapReduce
  cd MapReduce
  perl Makefile.PL
  make test

There is no make install step. You just copy bin/mr to your ~/bin/ dir, and copy test.conf to /etc/mapreduce.conf and edit to taste.

The B<make test> will test on your master machine as a single machine cluster. To add machines to your cluster, continue with the steps below.


=head1 ADDING MACHINES

How to gather machines into a cluster.

1. Edit /etc/mapreduce.conf to provide "hosts" entries, one for each host in the pool. 

2. Distribute shared keys to the other hosts. 

 # mr -cmd="mkdir .ssh; chmod 0700 .ssh"
 # mr -cmd="scp masterhost:.ssh/* .ssh/."

Distribute the host list.

 # mr -cmd="scp masterhost:.ssh/known_hosts .ssh/."

NOTE: Please see Hack #66 in the excellent B<Linux Server Hacks> by Rob Flickenger for details beyond the above instructions.
 
3. Place a MapReduce.pm install dir in the home directory of each mapreduce user, and rename it so there is no version number. This can be achieved like this from the master account (after the above steps): 

  # mr -cmd="scp masterhost:MapReduce-0.01.tar.gz ."
  # mr -cmd="tar zxvf MapReduce*"
  # mr -cmd="mv MapReduce-0.01 MapReduce"

4. Test the cluster integrity: cd back into the MapReduce/ install dir, copy B</etc/mapreduce.conf> to B<test.conf> and rerun B<make test> to check the cluster. When adding additional machines, repeat the above steps, and just remember all hosts must share the same ssh keypair, and they all have to have entries for everyone in their .ssh/known_hosts file.



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

There are two implementations of this model (that I know of): Google's original, and a Lucene-related project called B<Hadoop>. Google wrote theirs in C++, while the Lucene authors used Java.

Compared to the other, much more involved versions, this is more or less a toy. It's only suitable for smaller clusters, for various reasons.

It's based not on listening services per host, but instead on remote command invocation from a master program which forks processes to babysit the tasks. This makes the code rather simple and requires no true installation on any host in the pool. On the other hand it limits the number of hosts to available process memory on the master machine. Plus any network trouble is going to mess everything up.

A quite significant difference is the absense of a distributed file system. Google uses "GFS" to make input files available to map tasks, while this implementation simply stores distributed files across the cluster so that each host is responsible for the files on its local storage. This requires a preliminary "distribution step" which partitions a set of records across all hosts. This is quite simple since it can be implemented as a MapReduce operation! See 'partition_trec_docs.mr' and 'partition_lines.mr' in the bin directory for examples. This turns out to be more convenient than it looks, because often you will want to perform another mapreduce operation on the results of the last, and in this way the files are already distributed.

I haven't looked at Hadoop very much. Only to note that it appears much more "enterprise", which is to say it is more complicated. It has four xml configuration files (MapReduce.pm has only one, on one machine), and ... a lot of source code. It's no doubt much more stable, and also harder to get started. Not to mention... there are no Perl bindings.

Just to natter on a bit on relative complexity. The current Hadoop build has 313 java files with 61,810 lines of code. This doesn't include non-java files. There are 14 jsp files, 13 sh files, and 586 html files, plus. You could not check out the source code and understand everything in half a day. I'm not disparaging the Hadoop effort at all. It's probably very high quality. I'm just saying the concept is very simple, and it might be possible to have a simple yet stable implementation.

Even though this version is probably not as stable as the others, it comprises just over 500 lines of code as of the current release. It has only one configuration file, can be used almost immediately with very minimal effort, and is rather noninvasive, requiring nothing but untar-ing a directory on pool machines.

In short, perfect for exploring, understanding, and profitably using the MapReduce concepts with near-trivial time investment.

It's my goal to have a very sound system with comparable performance and stability to Google or Hadoop, using so little code that it can be read and understood in less than a day by a normal programmer.


=head1 LOAD BALANCING and FAULT TOLERANCE

Google and Hadoop deal with these issues separately, using a distributed file system for load balancing and centralized tracking of tasks for fault tolerance. I don't particularly like those because they're complicated and would require a lot more code than I feel is really necessary. I'm trying the idea of "replicated responsibility" to handle both load balancing and fault tolerance, and so far it works rather well and takes little code.

In MapReduce.pm, the input data are distributed evenly across hosts with a tunable B<replication factor> F which garuantees that F-1 hosts can go down at any time and the operation will still complete. The number of hosts that can B<actually> go down is larger, and is related to the total amount of input data, but I have yet to complete an analysis of what that is. The concept of B<shared responsibility> provides load balancing, in that each machine is responsible for making sure all files residing locally are processed, and will step up to do the processing themselves if given the opportunity (ie if finishing previous files faster than its brothers). This is simpler than centralized tracking.



=head1 TO DO

It would be nice to reduce the complexity of installation and deployment. I'm working on that.

I would like to improve MapReduce.pm as much as possible while still conforming to the goals of simplicity, stability and performance. Please, if you have suggestions for elegant improvements, get in touch with me.


=head1 SEE ALSO

http://labs.google.com/papers/mapreduce.html

Linux Server Hacks, Rob Flickenger


=head1 AUTHOR

Ira Woodhead, E<lt>ira at sweetpota dot toE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by Ira Woodhead

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.6 or,
at your option, any later version of Perl 5 you may have available.


=cut

