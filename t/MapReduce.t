# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MapReduce.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 2;
BEGIN { use_ok('MapReduce') };

#########################

# Perform two mapreduce operations: one to distribute 
# The Odyssey randomly line-by-line across all hosts; 
# one to do a unique word count on those lines.
# Then compare the results to the known correct ones.


system "rm -rf t/odyssey_*";
system "perl bin/mr -conf=test.conf -cmd='mkdir /tmp/mr'";
system "perl bin/mr -conf=test.conf -cmd='rm -r /tmp/mr/odyssey*'";
system "cp -r t/odyssey /tmp/mr/.";

system "perl bin/mr -conf=test.conf bin/partition_lines.mr odyssey odyssey_lines";
system "perl bin/mr -conf=test.conf bin/wc.mr odyssey_lines odyssey_wc";
system "rm -rf t/odyssey_result";
system "perl bin/mr -conf=test.conf -gather odyssey_wc t/odyssey_result";
system "gunzip t/odyssey_result/*gz";
system "sort -nrk2,2 t/odyssey_result/* > t/od-wc-testresult.txt";

@difflines = `diff t/od-wc-testresult.txt t/od-wc.txt`;


ok( @difflines == 0, "odyssey word count differs by " . scalar @difflines );



